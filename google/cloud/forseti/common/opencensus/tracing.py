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

import functools
import inspect
from google.cloud.forseti.common.util import logger

LOGGER = logger.get_logger(__name__)
DEFAULT_INTEGRATIONS = ['requests', 'sqlalchemy']

try:
    from opencensus.common.transports import async
    from opencensus.trace import config_integration
    from opencensus.trace import execution_context
    from opencensus.trace.exporters import file_exporter
    from opencensus.trace.exporters import stackdriver_exporter
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
    interceptor = client_interceptor.OpenCensusClientInterceptor(
        tracer,
        host_port=endpoint)
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
        transport = async.AsyncTransport

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
        tracer (opencensus.trace.tracer.Tracer): OpenCensus tracer object.
        module (str): The module name.
        function (str): The function name.
        kind (opencensus.trace.span.SpanKind): The span kind.
    """
    if tracer is not None:
        if kind is None:
            kind = SpanKind.SERVER
        span = tracer.start_span()
        span.name = '[{}] {}'.format(module, function)
        span.span_kind = kind
        tracer.add_attribute_to_current_span('module', module)
        tracer.add_attribute_to_current_span('function', function)
        LOGGER.debug('%s.%s: %s', module, function, tracer.span_context)


def end_span(tracer, **kwargs):
    """End a span.

    Args:
        tracer (opencensus.trace.tracer.Tracer): OpenCensus tracer object.
        kwargs (dict): A set of attributes to set to the current span.
    """
    if tracer is not None:
        LOGGER.debug(tracer.span_context)
        set_attributes(tracer, **kwargs)
        tracer.end_span()


# pylint: disable=broad-except
def set_attributes(tracer, **kwargs):
    """Set current span attributes.

    Args:
        tracer (opencensus.trace.tracer.Tracer): OpenCensus tracer object.
        kwargs (dict): A set of attributes to set to the current span.
    """
    for key, value in kwargs.items():
        try:
            tracer.add_attribute_to_current_span(key, value)
        except Exception:
            LOGGER.debug('Could not set attribute %s=%s to current span',
                         key, value)


def get_tracer(inst, attr=None):
    """Get a tracer from the current context.

    This function can get a tracer from any instance attribute if `attr` is
    passed.

    Otherwise, it will look for a tracer in the `default_attributes` before
    falling back on the OpenCensus execution context tracer.

    Args:
        inst (Object): An instance of a class.
        attr (str): The attribute to get / set the tracer from / to.

    Returns:
        tracer(opencensus.trace.Tracer): The tracer to be used.
    """
    default_attributes = ['tracer', 'config.tracer']
    tracer = None
    if OPENCENSUS_ENABLED:

        if attr is not None:  # Get tracer from passed attribute
            tracer = rgetattr(inst, attr, None)

        if tracer is None:  # Get tracer from standard attributes
            for _ in default_attributes:
                tracer = rgetattr(inst, _, None)

        if tracer is None:  # Get tracer from context
            tracer = execution_context.get_opencensus_tracer()

        # Set tracer if 'attr' was passed
        if tracer is not None and attr is not None:
            rsetattr(inst, attr, tracer)

        LOGGER.debug('%s: %s', inst, tracer.span_context)

    return tracer


def traced(cls):
    """Class decorator.

    Args:
        cls (object): Class to decorate.

    Returns:
        object: Decorated class.
    """
    for name, func in inspect.getmembers(cls, inspect.ismethod):
        setattr(cls, name, trace_decorator(func))
    return cls


def trace_decorator(func):
    """Method decorator to trace a class method.

    Args:
        func (func): Class method to be traced.

    Returns:
        wrapper: Decorated class method.
    """

    def wrapper(self, *args, **kwargs):
        """Wrapper method.

        Args:
            *args: Argument list passed to the method.
            **kwargs: Argument dict passed to the method.

        Returns:
            func: Function.
        """
        if OPENCENSUS_ENABLED:
            tracer = execution_context.get_opencensus_tracer()
            LOGGER.debug('%s.%s: %s', func.__module__, func.__name__,
                         tracer.span_context)
            if hasattr(self, 'config'):
                self.config.tracer = tracer
            else:
                self.tracer = tracer
        return func(self, *args, **kwargs)
    return wrapper


def trace(attr=None):
    """Decorator to trace class methods.

    Args:
        attr (str): The attribute to fetch from the instance.

    Returns:
        func: The decorated class method.
    """
    def decorator(func):
        """Method decorator.

        Args:
            func (func): Function to be decorated.

        Returns:
            func: Decorated function.
        """
        def wrapper(self, *args, **kwargs):
            """Method wrapper.

            Args:
                *args: Argument list passed to the function.
                **kwargs: Argument dictionary passed to the function.

            Returns:
                object: Function's return value.
            """
            if OPENCENSUS_ENABLED:
                tracer = get_tracer(self, attr)
                module_str = func.__module__.split('.')[-1]
                start_span(tracer, module_str, func.__name__)
            result = func(self, *args, **kwargs)
            if OPENCENSUS_ENABLED:
                end_span(tracer, result=result)
            return result
        return wrapper
    return decorator


def rsetattr(obj, attr, val):
    """Set nested attribute in object.

    Args:
        obj (Object): An instance of a class.
        attr (str): The attribute to set the tracer to.
        val (opencensus.trace.Tracer): The tracer to set attr to.

    Returns:
        object: Fetches attributes and sets it to object.
    """

    pre, _, post = attr.rpartition('.')
    return setattr(rgetattr(obj, pre) if pre else obj, post, val)

def rgetattr(obj, attr, *args):
    """Get nested attribute in object.

    Args:
        obj (Object): An instance of a class.
        attr (str): The attribute to get the tracer from.
        *args: Argument list passed to a function.

    Returns:
        object: Fetches attributes.
    """
    def _getattr(obj, attr):
        """Get attributes in object.

        Args:
            obj (Object): An instance of a class.
            attr (str): The attribute to get the tracer from.

        Returns:
            object: Fetches attributes to set to object.
        """
        return getattr(obj, attr, *args)
    return functools.reduce(_getattr, [obj] + attr.split('.'))
