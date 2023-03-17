def build_standards_links(rule, library_data_service):
    links = []
    for standard in rule.standards:
        standard_name = standard.get("Name", "").lower()
        standard_version = standard.get("Version", "").replace(".", "-")
        href = f"/mdr/{standard_name}/{standard_version}"
        try:
            data = library_data_service.get_api_json(href)
            links.append(data.get("_links", {}).get("self"))
        except Exception:
            pass
    return links
