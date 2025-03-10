import re
from typing import Tuple

def parse_identifier(identifier: str) -> Tuple[str, str]:
    if "ScopusID" in identifier:
        return "scopusID", identifier.split(": ")[1]
    elif "ResearcherID" in identifier:
        return "researcherID", identifier.split(": ")[1]
    elif "orcid" in identifier:
        return "orcid", identifier
    elif "ICO" in identifier:
        return "ICO", identifier.split(": ")[1]
    elif "ror" in identifier:
        return "ROR", identifier
    else:
        raise ValueError(f"Undefined scheme for the identifier: {identifier}")
    
def parse_personal_name(name: str) -> Tuple[str, str]:
    names = name.split(",")
    family_name = names[0].strip()
    given_name = "".join(names[1:]).strip(",").strip()
    return given_name, family_name

def parse_issn(value, identifiers):
    for vv in re.split("[,;]", value):
        vv = vv.strip()
        if vv.lower().startswith("issn:"):
            vv = vv[5:].strip()
        if vv.lower().startswith("issn"):
            vv = vv[4:].strip()
        vv = re.sub("[^a-zA-Z0-9-]", "", vv)
        if not vv or vv == "N":
            continue
        identifiers.append(_create_identifier_object("ISSN", vv))

def parse_isbn(value, identifiers):
   for isbn in re.split("[,;]", value):
       isbn = (isbn.strip()
                  .lower()
                  .replace("(cz)", "")
                  .replace("(en)", "")
                  .strip("()")
                  .removeprefix("isbn:")
                  .removeprefix("isbn")
                  .strip())
       if isbn and isbn != "n":
           identifiers.append(_create_identifier_object("ISBN", isbn))
           
def parse_rights(text):
    right = rights_dict.get(text)
    if not right:
        return None
    return vocabulary_cache.by_id("rights", "id")[right]