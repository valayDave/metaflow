import os
from .card import MetaflowCard, MetaflowCardComponent
from metaflow.extension_support import get_modules, EXT_PKG


def iter_namespace(ns_pkg):
    # Specifying the second argument (prefix) to iter_modules makes the
    # returned name an absolute name instead of a relative one. This allows
    # import_module to work without having to do additional modification to
    # the name.
    import pkgutil

    return pkgutil.iter_modules(ns_pkg.__path__, ns_pkg.__name__ + ".")


def _get_external_card_packages(with_paths=False):
    """
    Safely extract all exteranl card modules
    Args:
        with_paths (bool, optional): setting `with_paths=True` will result in a list of tuples: `[( mf_extensions_parent_path , relative_path_to_module, module)]`. setting false will return a list of modules Defaults to False.

    Returns:
        `list` of `ModuleType` or `list` of `tuples` where each tuple if of the form (mf_extensions_parent_path , relative_path_to_module, ModuleType)
    """
    import importlib

    available_card_modules = []
    for m in get_modules("plugins.cards"):
        # Iterate submodules of metaflow_extensions.X.plugins.cards
        # For example metaflow_extensions.X.plugins.cards.monitoring
        card_packages = [
            importlib.import_module(card_mod)
            if not with_paths
            else (fndx.path, importlib.import_module(card_mod))
            for fndx, card_mod, ispkg_c in iter_namespace(m.module)
            if ispkg_c
        ]
        if with_paths:
            card_packages = [
                (
                    os.path.abspath(
                        os.path.join(pth, "../../../../")
                    ),  # parent path to metaflow_extensions
                    os.path.join(
                        EXT_PKG,
                        os.path.relpath(m.__path__[0], os.path.join(pth, "../../../")),
                    ),  # construct relative path to parent.
                    m,
                )
                for pth, m in card_packages
            ]
        available_card_modules.extend(card_packages)
    return available_card_modules


def _load_external_cards():
    # Load external card packages
    card_packages = _get_external_card_packages()
    if not card_packages:
        return []
    external_cards = {}
    card_arr = []
    # Load cards from all external packages.
    for package in card_packages:
        try:
            cards = package.CARDS
            # Ensure that types match.
            if not type(cards) == list:
                continue
        except AttributeError:
            continue
        else:
            for c in cards:
                if not isinstance(c, type) or not issubclass(c, MetaflowCard):
                    # every card should only be inheriting a MetaflowCard
                    continue
                if not getattr(c, "type", None):
                    # todo Warn user of non existant `type` in MetaflowCard
                    continue
                if c.type in external_cards:
                    # todo Warn user of duplicate card
                    continue
                # external_cards[c.type] = c
                card_arr.append(c)
    return card_arr


def _get_external_card_package_paths():
    pkg_iter = _get_external_card_packages(with_paths=True)
    if pkg_iter is None:
        return None
    for (
        mf_extension_parent_path,
        relative_path_to_module,
        _,
    ) in pkg_iter:
        module_pth = os.path.join(mf_extension_parent_path, relative_path_to_module)
        arcname = relative_path_to_module
        yield module_pth, arcname


MF_EXTERNAL_CARDS = _load_external_cards()
