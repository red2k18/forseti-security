# Copyright 2017 The Forseti Security Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Forseti OpenCensus gRPC tracing setup."""

import inspect
from google.cloud.forseti.common.util import logger
import functools

LOGGER = logger.get_logger(__name__)
DEFAULT_INTEGRATIONS = ['requests', 'sqlalchemy', 'threading']

try:
    from opencensus.trace import config_integration
    from opencensus.trace import execution_context
    from opencensus.trace.exporters import file_exporter
    from opencensus.trace.exporters import stackdriver_exporter
    from opencensus.trace.exporters.transports import background_thread
    from opencensus.trace.ext.grpc import client_interceptor
    from opencensus.trace.ext.grpc import server_interceptor
    from opencensus.trace.samplers import always_on
    from opencensus.trace.tracer import Tracer
    from opencensus.trace.span import SpanKind
    OPENCENSUS_ENABLED = True
except ImportError:
    LOGGER.warning(
        'Cannot enable tracing because the `opencensus` library was not '
        'found. Run `pip install .[tracing]` to install tracing libraries.')
    OPENCENSUS_ENABLED = False


def create_client_interceptor(endpoint):
    """Create gRPC client interceptor.

    Args:
        endpoint (str): The gRPC channel endpoint (e.g: localhost:5001).

    Returns:
        OpenCensusClientInterceptor: a gRPC client-side interceptor.
    """
    exporter = create_exporter()
    tracer = Tracer(exporter=exporter)
    #LOGGER.info("before init: %s" % tracer.span_context)
    interceptor = client_interceptor.OpenCensusClientInterceptor(
        tracer,
        host_port=endpoint)
    #LOGGER.info("after init: %s" % execution_context.get_opencensus_tracer().span_context)
    return interceptor

def create_server_interceptor(extras=True):
    """Create gRPC server interceptor.

    Args:
        extras (bool): If set to True, also trace integration libraries.

    Returns:
        OpenCensusServerInterceptor: a gRPC server-side interceptor.
    """

    exporter = create_exporter()
    sampler = always_on.AlwaysOnSampler()
    if extras:
        trace_integrations()
    interceptor = server_interceptor.OpenCensusServerInterceptor(
        sampler,
        exporter)
    LOGGER.info(execution_context.get_opencensus_tracer().span_context)
    return interceptor

def trace_integrations(integrations=None):
    """Add tracing to supported OpenCensus integration libraries.

    Args:
        integrations (list): A list of integrations to trace.

    Returns:
        list: The integrated libraries names. The return value is used only for
            testing.
    """
    if integrations is None:
        integrations = DEFAULT_INTEGRATIONS
    tracer = execution_context.get_opencensus_tracer()
    integrated_libraries = config_integration.trace_integrations(
        integrations,
        tracer)
    LOGGER.info('Tracing integration libraries: %s', integrated_libraries)
    LOGGER.info(tracer.span_context)
    return integrated_libraries


def create_exporter(transport=None):
    """Create an exporter for traces.

    The default exporter is the StackdriverExporter. If it fails to initialize,
    the FileExporter will be used instead.

    Args:
        transport (opencensus.trace.exporters.transports.base.Transport): the
            OpenCensus transport used by the exporter to emit data.

    Returns:
        StackdriverExporter: A Stackdriver exporter.
        FileExporter: A file exporter. Default path: 'opencensus-traces.json'.
    """
    if transport is None:
        transport = background_thread.BackgroundThreadTransport

    try:
        exporter = stackdriver_exporter.StackdriverExporter(transport=transport)
        LOGGER.info(
            'StackdriverExporter set up successfully for project %s.',
            exporter.project_id)
        return exporter
    except Exception:  # pylint: disable=broad-except
        LOGGER.exception(
            'StackdriverExporter set up failed. Using FileExporter.')
        return file_exporter.FileExporter(transport=transport)


def start_span(tracer, module, function, kind=None):
    """Start a span.

    Args:
        tracer (~opencensus.trace.tracer.Tracer): OpenCensus tracer object.
        module (str): The module name.
        function (str): The function name.
        kind (~opencensus.trace.span.SpanKind, optional): The span kind.
    """
    LOGGER.info("%s.%s: %s", module, function, tracer.span_context)
    if kind is None:
        kind = SpanKind.SERVER
    span = tracer.start_span()
    span.name = "[{}] {}".format(module, function)
    span.span_kind = kind
    tracer.add_attribute_to_current_span('module', module)
    tracer.add_attribute_to_current_span('function', function)
    return span


def end_span(tracer, **kwargs):
    """End a span.

    Args:
        tracer (~opencensus.trace.tracer.Tracer): OpenCensus tracer object.
        kwargs (dict): A set of attributes to set to the current span.
    """
    LOGGER.info(tracer.span_context)
    set_attributes(tracer, **kwargs)
    tracer.end_span()


def set_attributes(tracer, **kwargs):
    """Set attributes for the current span.

    Args:
        tracer (~opencensus.trace.tracer.Tracer): OpenCensus tracer object.
        kwargs (dict): A set of attributes to set to the current span.
    """
    for k, v in kwargs.items():
        tracer.add_attribute_to_current_span(k, v)


def traced(attr='tracer', _lambda=None, methods='all'):
    """Decorate selected class methods to trace.

    Args:
        attr (`string`): If passed, get/set the OpenCensus tracer from the
                         instance attribute.
        _lambda (`function`): If passed, fetch the OpenCensus tracer from the
                              lambda function.

    Returns:
       `object`: The decorated class.
    """
    def wrapper(cls):
        for name, fn in inspect.getmembers(cls, inspect.ismethod):
            if name == '__init__':
                continue
            if methods == 'all' or (isinstance(methods, list) and name in methods):
                LOGGER.info("Decorating %s.%s for tracing.", cls.__name__, name)
                setattr(cls, name, trace(attr, _lambda)(fn))
        return cls
    return wrapper


def trace(attr='tracer', _lambda=None):
    """Decorator to trace class methods.

    If nothing is passed,

    Args:
        attr (`string`): If passed, get / set the OpenCensus tracer to the
                         instance attribute.
        _lambda (`function`): If passed, fetch the OpenCensus tracer from the
                              lambda function.
    Returns:
        `function`: The decorated method.
    """
    def outer_wrapper(f):
        def inner_wrapper(self, *args, **kwargs):
            if OPENCENSUS_ENABLED:
                tracer = get_class_tracer(self, attr, _lambda)
                module_str = f.__module__.split('.')[-1]
                LOGGER.info("%s.%s: %s", module_str, f.__name__, tracer.span_context)
                start_span(tracer, module_str, f.__name__)

            result = f(self, *args, **kwargs)

            if OPENCENSUS_ENABLED:
                end_span(tracer, result=result)

            return result
        return inner_wrapper
    return outer_wrapper


def get_class_tracer(self, attr='tracer', _lambda=None):
    """Get OpenCensus tracer for a class.

    This method is used to get / set the tracer when working in the context of
    a class.

    Get a tracer from the current context (such as an attribute of the class
    instance, or a lambda function), and to set it to` the desired attribute.

    If none of `get_attr`, `set_attr` or `_lambda` is passed, get the tracer
    from OpenCensus context.

    Args:
        attr (`string`): If passed, get/set the OpenCensus tracer from the
                         instance attribute. Support nested attributes.
        _lambda (`function`): If passed, fetch the OpenCensus tracer from the
                              lambda function.
    """
    if _lambda is not None: # get tracer from lambda
        LOGGER.debug("Getting tracer from lambda function.")
        tracer = _lambda(self)

    elif attr is not None: # get tracer from instance attribute (support nested)
        LOGGER.debug("Getting tracer from 'self.%s'." % attr)
        tracer = rgetattr(self, attr, None)

    if tracer is None: # get tracer from OpenCensus context
        LOGGER.info("Getting tracer from OpenCensus execution context")
        tracer = execution_context.get_opencensus_tracer()

    # set tracer to instance attribute (support nested)
    LOGGER.info("Setting tracer to 'self.%s'", attr)
    rsetattr(self, attr, tracer)

    return tracer


def rsetattr(obj, attr, val):
    """Set nested attribute in object."""
    pre, _, post = attr.rpartition('.')
    return setattr(rgetattr(obj, pre) if pre else obj, post, val)


def rgetattr(obj, attr, *args):
    """Get nested attribute in object."""
    def _getattr(obj, attr):
        return getattr(obj, attr, *args)
    return functools.reduce(_getattr, [obj] + attr.split('.'))

# def trace(_lambda=None, attr=None):
#     """Decorator to trace class methods.

#     This decorator expect the tracer is set in the class via an instance attribute,
#     or is fetchable using a lambda function.

#     If nothing is passed to the decorator, it will use the execution context to get
#     the tracer.

#     Args:
#         _lambda (func, optional): A lambda definition defining how to get the tracer.
#         attr (str, optional): The attribute to fetch from the instance.

#     Returns:
#         func: The decorated class method.
#     """
#     def decorator(func):
#         def wrapper(self, *args, **kwargs):
#             if OPENCENSUS_ENABLED:
#                 if _lambda is not None:
#                     tracer = _lambda(self)
#                 elif attr is not None:
#                     tracer = getattr(self, attr, None)
#                 else: # no arg passed to decorator, getting tracer from context
#                     tracer = execution_context.get_opencensus_tracer()
#                 module_str = func.__module__.split('.')[-1]
#                 span = start_span(tracer, module_str, func.__name__)
#             result = func(self, *args, **kwargs)
#             if OPENCENSUS_ENABLED:
#                 end_span(tracer, result=result)
#         return wrapper
#     return decorator
