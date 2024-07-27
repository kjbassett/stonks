import importlib
import inspect
import os
import types

from icecream import ic


def load_plugin_metadata(func):
    """
    example metadata structure:
    metadata = {
        name,
        doc,
        arguments: [
            {
                name,
                type,
                default,
                decorator_metadata['name'] if name in decorator_metadata
            }],
        return_type,
        **decorator_metadata
    }
    """
    signature = inspect.signature(func)

    metadata = {
        "is_plugin": True,
        "name": func.__name__,
        "doc": func.__doc__,
        "arguments": [],
        "return_type": str(signature.return_annotation.__name__),
    }

    metadata.update(func.decorator_metadata)

    for name, param in signature.parameters.items():
        if isinstance(param.annotation, types.UnionType):
            _type = "any"
        else:
            _type = param.annotation.__name__
        arg_info = {
            "name": name,
            "type": _type,
            "default": (
                param.default if param.default is not inspect.Parameter.empty else None
            ),
        }
        arg_info.update(func.decorator_metadata.get(name, {}))
        metadata["arguments"].append(arg_info)

    return metadata


def load_plugins(folder="plugins"):
    # metadata matches folder structure of plugin folder and holds metadata for each plugin. Used for webpage
    metadata = {}
    plugins = {}  # dict of plugin functions

    for root, dirs, files in os.walk(folder):
        for file in files:
            ic(root, file)
            if file.endswith(".py") and file != "__init__.py":
                relative_path = os.path.relpath(root, folder)
                if relative_path == ".":
                    relative_path = ""
                else:
                    relative_path = "." + relative_path.replace(os.sep, ".")
                module_name = os.path.splitext(file)[0]
                import_path = f"{relative_path}.{module_name}"

                # ic(import_path)
                module = importlib.import_module(import_path, package=folder)
                print(import_path)

                for name, func in inspect.getmembers(module, inspect.isfunction):
                    if getattr(func, "is_plugin", False):
                        # add plugin function to plugins
                        import_path = import_path.strip(".")
                        func_path = f"{import_path}.{name}"
                        plugins[func_path] = {"function": func, "task": None}

                        # add plugin metadata to folder-structured metadata
                        parts = import_path.split(".")
                        print(parts)
                        current_level = metadata
                        # Recursively enter/create folder structure to put metadata in correct spot
                        for part in parts:
                            current_level = current_level.setdefault(part, {})
                        current_level[name] = load_plugin_metadata(func)
                        current_level[name]["id"] = func_path

    ic(metadata)
    ic(plugins)
    return metadata, plugins
