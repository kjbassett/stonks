def plugin(**decorator_metadata):
    def decorator(func):
        func.is_plugin = True
        func.decorator_metadata = decorator_metadata
        return func

    return decorator
