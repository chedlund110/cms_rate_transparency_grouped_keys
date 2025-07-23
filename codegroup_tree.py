from context import Context
from term_bundle import TermBundle
from utilities import get_dict_value, normalize_code_type
from collections import defaultdict
from itertools import product
import re

def extract_provider_ranges_from_tree(tree: dict, context: Context) -> dict:
    provider_ranges = defaultdict(dict)

    def walk(node):
        for child in node.get("children", []):
            if "nested_group" in child:
                walk(child["nested_group"])
            else:
                code_type = child.get("code_type")
                code_low = child.get("code_low")
                is_not = child.get("not_logic_ind", False)

                # ✅ Only keep if it's a provider-level type
                if code_type in context.provider_code_range_types and code_low:
                    provider_ranges[code_type][code_low] = {"not_logic_ind": is_not}

    walk(tree)
    return provider_ranges

def expand_zip_code_range(code_low: str, code_high: str) -> list[str]:

    code_low = str(code_low)
    code_high = str(code_high)
    
    zip_low_match = re.match(r"\d{5}", code_low)
    zip_high_match = re.match(r"\d{5}", code_high)

    if not zip_low_match or not zip_high_match:
        raise ValueError(f"Invalid ZIP code range: {code_low} to {code_high}")

    zip_low_5 = zip_low_match.group(0)
    zip_high_5 = zip_high_match.group(0)

    low = int(zip_low_5)
    high = int(zip_high_5)

    if high > 99999 or high < low or high - low > 1000:
        raise ValueError(f"ZIP range too large or invalid: {zip_low_5}–{zip_high_5}")

    return [str(i).zfill(5) for i in range(low, high + 1)]

def build_code_group_tree_from_term(context: Context, term_bundle: TermBundle) -> dict:

    """
    Builds and caches a code group tree for the given term.

    If the code group has already been processed, the function returns the cached
    version from context. Otherwise, it recursively builds the tree based on the
    structure in shared_config.codegroups and stores the result in context.codegroup_trees.

    Args:
        context (Context): The runtime environment containing shared config and caches.
        term_bundle (TermBundle): The term data specifying the code group to build.

    Returns:
        dict: A nested structure representing the code group tree.

    Raises:
        ValueError: If the specified code group ID is not found in shared_config.
    """

    code_group_id = term_bundle.code_group_id
    code_low = term_bundle.code_low
    code_high = term_bundle.code_high
    if str(code_high).isdigit():
        if int(code_high) > 99999:
            code_high = 99999
    code_type = term_bundle.code_type
    parent_code_group_id = term_bundle.parent_code_group_id

    # Base case: no code group
    if not code_group_id:
        return {
            "code_group_id": None,
            "code_group_name": None,
            "not_logic_ind": False,
            "children": [{
                "code_low": code_low,
                "code_high": code_high,
                "code_type": code_type,
                "not_logic_ind": False
            }] if code_low and code_high else [],
            "provider_ranges": defaultdict(dict)
        }

    # Prevent infinite recursion
    if code_group_id == parent_code_group_id:
        return {}

    # Return cached tree if already built
    if code_group_id in context.codegroup_trees:
        return context.codegroup_trees[code_group_id]

    group = context.shared_config.codegroups.get(code_group_id)
    if not group:
        raise ValueError(f"Code group {code_group_id} not found in shared_config.codegroups.")

    tree = {
        "code_group_id": code_group_id,
        "code_group_name": group["code_group_name"],
        "not_logic_ind": False,
        "children": [],
        "provider_ranges": defaultdict(dict)
    }

    for row in group["values"]:
        nested_group_id = row.get("nested_code_group_id")
        code_type = row.get("code_type")
        code_low = row.get("code_low")
        code_high = row.get("code_high")
        is_not = row.get("not_logic_ind", False)

        # Handle provider-level ranges
        if code_type in context.provider_code_range_types and code_low:
            tree["provider_ranges"][code_type][code_low] = {
                "not_logic_ind": is_not
            }

        if nested_group_id:
            nested_term = {
                "CODEGROUPID": nested_group_id,
                "CODELOWVALUE": code_low,
                "CODEHIGHVALUE": code_high,
                "CODETYPEBEAN": code_type
            }
            nested_bundle = TermBundle(nested_term, parent_code_group_id=code_group_id)
            child_tree = build_code_group_tree_from_term(context, nested_bundle)
            tree["children"].append({
                "nested_group": child_tree,
                "not_logic_ind": is_not
            })

        elif code_low and code_high:
            tree["children"].append({
                "code_low": code_low,
                "code_high": code_high,
                "code_type": code_type,
                "not_logic_ind": is_not
            })

    # Cache built tree
    context.codegroup_trees[code_group_id] = tree
    return tree

def generate_service_combinations(context: Context, tree: dict) -> dict:
    def extract_codes(node: dict):
        service_codes_by_type = defaultdict(set)
        excluded_services_by_type = defaultdict(set)
        modifiers = set()
        excluded_modifiers = set()
        pos_values = set()
        excluded_pos_values = set()

        for child in node.get("children", []):
            if "nested_group" in child:
                sub_result = extract_codes(child["nested_group"])

                for k in sub_result["services"]:
                    service_codes_by_type[k].update(sub_result["services"][k])
                    excluded_services_by_type[k].update(sub_result["excluded_services"].get(k, set()))

                modifiers.update(sub_result["modifiers"])
                excluded_modifiers.update(sub_result["excluded_modifiers"])
                pos_values.update(sub_result["pos"])
                excluded_pos_values.update(sub_result["excluded_pos"])
            else:
                code_type = child.get("code_type")
                code_low = child.get("code_low")
                code_high = child.get("code_high")
                if str(code_high).isdigit() and int(code_high) > 99999:
                    code_high = 99999
                is_not = child.get("not_logic_ind", False)

                if not code_type or not code_low:
                    continue

                if code_low == code_high:
                    codes = {code_low}
                else:
                    try:
                        if code_type == "CodeTypeProviderZip":
                            codes = set(expand_zip_code_range(code_low, code_high))
                        else:
                            codes = {str(i) for i in range(int(code_low), int(code_high) + 1)}
                    except ValueError:
                        codes = {code_low}

                if code_type in context.service_code_range_types:
                    (excluded_services_by_type if is_not else service_codes_by_type)[code_type].update(codes)
                elif code_type == "CodeTypeCPTMod":
                    (excluded_modifiers if is_not else modifiers).update(codes)
                elif code_type == "CodeTypePlaceOfService":
                    (excluded_pos_values if is_not else pos_values).update(codes)

        return {
            "services": service_codes_by_type,
            "excluded_services": excluded_services_by_type,
            "modifiers": modifiers,
            "excluded_modifiers": excluded_modifiers,
            "pos": pos_values,
            "excluded_pos": excluded_pos_values
        }

    result = extract_codes(tree)

    included_modifiers = sorted(result["modifiers"] - result["excluded_modifiers"])
    included_pos = sorted(result["pos"] - result["excluded_pos"])

    if not included_modifiers:
        included_modifiers = ['']
    if not included_pos:
        included_pos = ['11']

    combinations = []
    has_services = False

    # --- Case 1: service code logic ---
    for code_type, svc_set in result["services"].items():
        filtered_svcs = sorted(svc_set - result["excluded_services"].get(code_type, set()))
        if filtered_svcs:
            has_services = True
        else:
            filtered_svcs = ['']
        combinations.extend(
            (svc, mod, pos, normalize_code_type(code_type))
            for svc in filtered_svcs
            for mod in included_modifiers
            for pos in included_pos
        )

    # --- Case 2: modifier-only fallback using modifier_map ---
    if not has_services and result["modifiers"]:
        modifier_map = context.shared_config.modifier_map
        for modifier in included_modifiers:
            service_codes = modifier_map.get(modifier, [])
            for proc_code in sorted(service_codes):
                combinations.extend(
                    (proc_code, modifier, pos, "CPT")
                    for pos in included_pos
                )
        if combinations:
            has_services = True

    # --- Case 3: POS-only fallback ---
    if not has_services and not result["modifiers"] and included_pos:
        combinations.extend(
            ('', '', pos, '') for pos in included_pos
        )
        # has_services remains False

    return {
        "combinations": combinations,
        "has_services": has_services
    }

"""
def generate_service_combinations(context: Context, tree: dict) -> dict:
    def extract_codes(node: dict):
        service_codes_by_type = defaultdict(set)
        excluded_services_by_type = defaultdict(set)
        modifiers = set()
        excluded_modifiers = set()
        pos_values = set()
        excluded_pos_values = set()

        for child in node.get("children", []):
            if "nested_group" in child:
                sub_result = extract_codes(child["nested_group"])
                for k in sub_result["services"]:
                    service_codes_by_type[k].update(sub_result["services"][k])
                    excluded_services_by_type[k].update(sub_result["excluded_services"].get(k, set()))
                modifiers.update(sub_result["modifiers"])
                excluded_modifiers.update(sub_result["excluded_modifiers"])
                pos_values.update(sub_result["pos"])
                excluded_pos_values.update(sub_result["excluded_pos"])
            else:
                code_type = child.get("code_type")
                code_low = child.get("code_low")
                code_high = child.get("code_high")
                if str(code_high).isdigit() and int(code_high) > 99999:
                    code_high = 99999
                is_not = child.get("not_logic_ind", False)

                if not code_type or not code_low:
                    continue

                if code_low == code_high:
                    codes = {code_low}
                else:
                    try:
                        if code_type == "CodeTypeProviderZip":
                            codes = set(expand_zip_code_range(code_low, code_high))
                        else:
                            codes = {str(i) for i in range(int(code_low), int(code_high) + 1)}
                    except ValueError:
                        codes = {code_low}

                if code_type in context.service_code_range_types:
                    (excluded_services_by_type if is_not else service_codes_by_type)[code_type].update(codes)
                elif code_type == "CodeTypeCPTMod":
                    (excluded_modifiers if is_not else modifiers).update(codes)
                elif code_type == "CodeTypePlaceOfService":
                    (excluded_pos_values if is_not else pos_values).update(codes)

        return {
            "services": service_codes_by_type,
            "excluded_services": excluded_services_by_type,
            "modifiers": modifiers,
            "excluded_modifiers": excluded_modifiers,
            "pos": pos_values,
            "excluded_pos": excluded_pos_values
        }

    result = extract_codes(tree)

    included_modifiers = sorted(result["modifiers"] - result["excluded_modifiers"])
    included_pos = sorted(result["pos"] - result["excluded_pos"])

    if not included_modifiers:
        included_modifiers = ['']
    if not included_pos:
        included_pos = ['11']

    combinations = []
    has_services = False

    # If there are direct service codes, generate combinations normally
    for code_type, svc_set in result["services"].items():
        filtered_svcs = sorted(svc_set - result["excluded_services"].get(code_type, set()))
        if filtered_svcs:
            has_services = True
        else:
            filtered_svcs = ['']
        combinations.extend(
            (svc, mod, pos, normalize_code_type(code_type))
            for svc in filtered_svcs
            for mod in included_modifiers
            for pos in included_pos
        )

    # Handle modifier-only case using modifier_map
    if not has_services and result["modifiers"]:
        modifier_map = context.shared_config.modifier_map
        for modifier in included_modifiers:
            service_codes = modifier_map.get(modifier, [])
            for proc_code in sorted(service_codes):
                combinations.extend(
                    (proc_code, modifier, pos, "CPT")  # assume CPT as default for modifier-based fallback
                    for pos in included_pos
                )
        if combinations:
            has_services = True
            
    return {
        "combinations": combinations,
        "has_services": has_services
    }

def generate_service_combinations(context: Context, tree: dict) -> dict:
    def extract_codes(node: dict):
        service_codes_by_type = defaultdict(set)
        excluded_services_by_type = defaultdict(set)
        modifiers = set()
        excluded_modifiers = set()
        pos_values = set()
        excluded_pos_values = set()

        for child in node.get("children", []):
            if "nested_group" in child:
                sub_result = extract_codes(child["nested_group"])

                for k in sub_result["services"]:
                    service_codes_by_type[k].update(sub_result["services"][k])
                    excluded_services_by_type[k].update(sub_result["excluded_services"].get(k, set()))

                modifiers.update(sub_result["modifiers"])
                excluded_modifiers.update(sub_result["excluded_modifiers"])
                pos_values.update(sub_result["pos"])
                excluded_pos_values.update(sub_result["excluded_pos"])
            else:
                code_type = child.get("code_type")
                code_low = child.get("code_low")
                code_high = child.get("code_high")
                if str(code_high).isdigit() and int(code_high) > 99999:
                    code_high = 99999
                is_not = child.get("not_logic_ind", False)

                if not code_type or not code_low:
                    continue

                if code_low == code_high:
                    codes = {code_low}
                else:
                    try:
                        if code_type == "CodeTypeProviderZip":
                            codes = set(expand_zip_code_range(code_low, code_high))
                        else:
                            codes = {str(i) for i in range(int(code_low), int(code_high) + 1)}
                    except ValueError:
                        codes = {code_low}

                if code_type in context.service_code_range_types:
                    (excluded_services_by_type if is_not else service_codes_by_type)[code_type].update(codes)
                elif code_type == "CodeTypeCPTMod":
                    (excluded_modifiers if is_not else modifiers).update(codes)
                elif code_type == "CodeTypePlaceOfService":
                    (excluded_pos_values if is_not else pos_values).update(codes)

        return {
            "services": service_codes_by_type,
            "excluded_services": excluded_services_by_type,
            "modifiers": modifiers,
            "excluded_modifiers": excluded_modifiers,
            "pos": pos_values,
            "excluded_pos": excluded_pos_values
        }

    result = extract_codes(tree)

    included_modifiers = sorted(result["modifiers"] - result["excluded_modifiers"])
    included_pos = sorted(result["pos"] - result["excluded_pos"])

    if not included_modifiers:
        included_modifiers = ['']
    if not included_pos:
        included_pos = ['11']

    combinations = []
    has_services = False

    for code_type, svc_set in result["services"].items():
        filtered_svcs = sorted(svc_set - result["excluded_services"].get(code_type, set()))
        if filtered_svcs:
            has_services = True
        else:
            filtered_svcs = ['']
        combinations.extend(
            (svc, mod, pos, normalize_code_type(code_type))
            for svc in filtered_svcs
            for mod in included_modifiers
            for pos in included_pos
        )

    return {
        "combinations": combinations,
        "has_services": has_services
    }


def generate_service_combinations(context: Context, tree: dict) -> dict:
    def extract_codes(node: dict):
        service_codes_by_type = defaultdict(set)
        excluded_services_by_type = defaultdict(set)
        modifiers = set()
        excluded_modifiers = set()
        pos_values = set()
        excluded_pos_values = set()

        for child in node.get("children", []):
            if "nested_group" in child:
                sub_result = extract_codes(child["nested_group"])

                for k in sub_result["services"]:
                    service_codes_by_type[k].update(sub_result["services"][k])
                    excluded_services_by_type[k].update(sub_result["excluded_services"].get(k, set()))

                modifiers.update(sub_result["modifiers"])
                excluded_modifiers.update(sub_result["excluded_modifiers"])
                pos_values.update(sub_result["pos"])
                excluded_pos_values.update(sub_result["excluded_pos"])

            else:
                code_type = child.get("code_type")
                code_low = child.get("code_low")
                code_high = child.get("code_high")
                if str(code_high).isdigit():
                    if int(code_high) > 99999:
                        code_high = 99999
                is_not = child.get("not_logic_ind", False)

                if not code_type or not code_low:
                    continue

                if code_low == code_high:
                    codes = {code_low}
                else:
                    try:
                        if code_type == "CodeTypeProviderZip":
                            codes = set(expand_zip_code_range(code_low, code_high))
                        else:
                            codes = {str(i) for i in range(int(code_low), int(code_high) + 1)}
                    except ValueError:
                        codes = {code_low}

                if code_type in context.service_code_range_types:
                    (excluded_services_by_type if is_not else service_codes_by_type)[code_type].update(codes)
                elif code_type == "CodeTypeCPTMod":
                    (excluded_modifiers if is_not else modifiers).update(codes)
                elif code_type == "CodeTypePlaceOfService":
                    (excluded_pos_values if is_not else pos_values).update(codes)

        return {
            "services": service_codes_by_type,
            "excluded_services": excluded_services_by_type,
            "modifiers": modifiers,
            "excluded_modifiers": excluded_modifiers,
            "pos": pos_values,
            "excluded_pos": excluded_pos_values
        }

    result = extract_codes(tree)

    included_services = sorted(set().union(*[
        result["services"][k] - result["excluded_services"].get(k, set())
        for k in result["services"]
    ]))
    included_modifiers = sorted(result["modifiers"] - result["excluded_modifiers"])
    included_pos = sorted(result["pos"] - result["excluded_pos"])

    if not included_modifiers:
        included_modifiers = ['']
    if not included_pos:
        included_pos = ['11']
    
    if not included_services:
        combinations = list(product([''], included_modifiers, included_pos))
        has_services = False
    else:
        combinations = list(product(included_services, included_modifiers, included_pos))
        has_services = True
    

    return {
        "combinations": combinations,
        "has_services": has_services
    }
"""