from datetime import datetime
import itertools
import re
from oarepo_oaipmh_harvester.transformers.rule import (
    OAIRuleTransformer,
    matches,
    deduplicate,
    ignore,
    make_dict,
    make_array,
)
from oarepo_runtime.datastreams.types import StreamEntry, StreamEntryFile
import pycountry
from typing import Dict, List, Optional, Tuple, Union

from invenio_cache.proxies import current_cache

import logging

import sqlalchemy
import Levenshtein
from invenio_search.engine import dsl

log = logging.getLogger("oaipmh.harvester")

# will increase this in production
DEFAULT_VOCABULARY_CACHE_TTL = 3600


def get_alpha2_lang(lang):
    py_lang = pycountry.languages.get(alpha_3=lang) or pycountry.languages.get(
        bibliographic=lang
    )
    if not py_lang:
        raise LookupError()
    return py_lang.alpha_2


class NUSLTransformer(OAIRuleTransformer):
    def transform(self, entry: StreamEntry):
        md = entry.transformed.setdefault("metadata", {})

        entry.transformed.setdefault("files", {})["enabled"] = False

        transform_001_control_number(md, entry)
        transform_020_isbn(md, entry)
        transform_022_issn(md, entry)
        transform_035_original_record_oai(md, entry)
        transform_046_date_modified(md, entry)
        transform_046_date_issued(md, entry)
        transform_245_title(md, entry)
        transform_245_translated_title(md, entry)
        transform_246_title_alternate(md, entry)
        transform_24633a_subtitle(md, entry)
        transform_24633b_subtitle(md, entry)
        transform_260_publisher(md, entry)
        transform_490_series(md, entry)
        transform_520_abstract(md, entry)
        transform_598_note(md, entry)
        transform_65007_subject(md, entry)
        transform_65017_subject(md, entry)
        transform_650_7_subject(md, entry)
        transform_6530_en_keywords(md, entry)
        transform_653_cs_keywords(md, entry)
        transform_7112_event(md, entry)
        transform_720_creator(md, entry)
        transform_720_contributor(md, entry)
        transform_7731_related_item(md, entry)
        transform_85640_original_record_url(md, entry)
        transform_85642_external_location(md, entry)
        transform_970_catalogue_sysno(md, entry)
        transform_980_resource_type(md, entry)
        transform_996_accessibility(md, entry)
        transform_999C1_funding_reference(md, entry)

        transform_04107_language(md, entry)
        transform_336_certifikovana_metodika(md, entry)

        transform_540_rights(md, entry)

        transform_oai_identifier(md, entry)

        transform_502_degree_grantor(md, entry)
        transform_7102_degree_grantor(md, entry)  # a a 9='cze'

        transform_502_date_defended(md, entry)

        transform_586_defended(md, entry)  # obhajeno == true
        transform_656_study_field(md, entry)

        transform_998_collection(md, entry)

        transform_856_attachments(md, entry)
        if not entry.transformed["files"]["enabled"] and "accessRights" not in md:
            # if no files and no accessibility was set, then set the access rights to metadata
            md["accessRights"] = { "id": access_right_dict["0"] }

        deduplicate(md, "languages")
        deduplicate(md, "contributors")
        deduplicate(md, "subjects")
        deduplicate(md, "additionalTitles")

        ignore(entry, "909COq")  # "licensed", "openaire", ...
        ignore(entry, "909COp")  # oai set
        ignore(entry, "909COo")  # oai identifier taken from elsewhere
        ignore(entry, "005")  # modification time
        ignore(entry, "502__b")  # titul
        ignore(entry, "502__g")  # treba "Magisterský studijní program"
        ignore(entry, "008")  # podivnost
        ignore(entry, "0248_a")  # nusl identifikator

        ignore(entry, "300")  # "extent"

        # # asi prilogy
        ignore(entry, "340__a")  # "text/pdf"
        ignore(entry, "506__a")  # "public"
        ignore(entry, "655_72")  # "NUŠL typ dokumentu"
        ignore(entry, "655_7a")  # "Disertační práce"
        # ignore(entry, "8564_u")  # odkaz na soubor
        # ignore(entry, "8564_z")  # nazev/typ souboru "plny text"
        ignore(entry, "8564_x")  # "icon"
        ignore(entry, "996__9")  # "0"
        ignore(entry, "656_72")  # "AKVO"
        ignore(
            entry, "500__a"
        )  # "BÍLEK, Martin. Hospodářská etika jako etika rámcového řádu. Kritická reflexe hospodářsko-etické koncepce Karla Homanna. Č. Budějovice, 2011. disertační práce (Th.D.). JIHOČESKÁ UNIVERZITA V ČESKÝCH BUDĚJOVICÍCH. Teologická fakulta",
        ignore(entry, "85642z")  # "Elektronické umístění souboru",
        ignore(entry, "502__d")  # "2007"
        ignore(entry, "586__b")  # "successfully defended",

        # ignore(entry, "720__e")  # "advisor", "referee"

        # ignore(entry, "6557_2")  # "NUŠL typ dokumentu"
        # ignore(entry, "6557_a")  # "Výzkumné zprávy",
        # ignore(entry, "999C1b")  # "GA AV ČR"
        # ignore(entry, "7731_x")  # "ISSN 1804–2406",
        # ignore(entry, "4900_v")  # "V-1110"
        # ignore(entry, "7112_c")  # "Praha (CZ)",
        # ignore(entry, "7112_d")  # "2010-12-08",
        # ignore(entry, "7731_z")  # "978-80-7375-514-0",
        # ignore(entry, "7112_d")  # "2008-08-24 / 2008-08-28",
        #
        # ignore(entry, "720__6")  # "https://orcid.org/0000-0002-8255-348X",
        # ignore(entry, "8564_y")  # "česká verze",
        # ignore(entry, "7731_g")  # "Česká národní banka",
        # ignore(entry, "FFT_0a")  # "http://pro.inflow.cz/projekt-informacniho-vzdelavani-pedagogu-na-stredni-technicke-skole"
        # ignore(entry, "246__n")  # "Podprojekt A",
        # ignore(entry, "7201_i")  # "Univerzita Karlova, Lékařská fakulta v Plzni",
        # ignore(entry, "24500 ")  # "12 zák. č. 144/1992/ Sb. o ochraně přírody a krajiny) na území v
        #
        # ignore(entry, "650_72")  # "PSH",
        # ignore(entry, "650_77")  # "nlk20040147082",
        # ignore(entry, "999c1a")  # "WP2-98"
        # ignore(entry, "999c1a")  # "WP2-98"
        # ignore(entry, "999c1b")  # "Ministerstvo zemědělství ČR",
        # ignore(entry, "4900_b")  # "4/2012",
        # ignore(entry, "7731_g")  # "Roč. 22, č. 2 (2011)",
        # ignore(entry, "999C19")  # "MŠMT ČR"
        # ignore(entry, "8564_y")  # "česká verze", "English version"
        # ignore(entry, "999C2a")  # "UK", "GA ČR"
        #
        #
        # ignore(entry, "24630a")  # "ročník 8, číslo 1",

        return True

@matches("8564_u", "8564_z", paired=True)
def transform_856_attachments(md, entry, value):
    link, _ = value
    filename = link.split("/")[-1]
    entry.files.append(StreamEntryFile({ "key": filename }, link))
    entry.transformed["files"]["enabled"] = True

    md["accessRights"] = { "id": access_right_dict["1"] }

@matches("001")
def transform_001_control_number(md, entry, value):
    md.setdefault("systemIdentifiers", []).append(
        _create_identifier_object("nusl", "http://www.nusl.cz/ntk/nusl-" + value)
    )


@matches("020__a")
def transform_020_isbn(md, entry, value):
    md.setdefault("objectIdentifiers", []).append(
        _create_identifier_object("ISBN", value)
    )


@matches("022__a")
def transform_022_issn(md, entry, value):
    md.setdefault("objectIdentifiers", []).append(
        _create_identifier_object("ISSN", value)
    )


@matches("035__a")
def transform_035_original_record_oai(md, entry, value):
    md.setdefault("systemIdentifiers", []).append(
        _create_identifier_object("originalRecordOAI", value)
    )


@matches("046__j")
def transform_046_date_modified(md, entry, value):
    if value is None:
        return
    
    md["dateModified"] = convert_to_date(value)


@matches("046__k")
def transform_046_date_issued(md, entry, value):
    if value is None:
        return

    if value.startswith("c"):
        value = value[1:]

    if len(value) == 8 and all(c.isdigit() for c in value):
        # iso 8601 date formats, e. g., YYYY-MM-DD or DD-MM-YYYY
        def is_valid_date(date_str, date_format):
            try:
                datetime.strptime(date_str, date_format)
                return True
            except ValueError:
                return False

        if is_valid_date(value, "%Y%m%d"):
            md["dateIssued"] = value[:4]
            return

        if is_valid_date(value, "%d%m%Y") or is_valid_date(value, "%m%d%Y"):
            md["dateIssued"] = value[-4:]
            return

    date_issued = convert_to_date(value)
    md["dateIssued"] = date_issued


@matches("24500a")
def transform_245_title(md, entry, value):
    if value is None:
        return

    md["title"] = value


@matches("24500b")
def transform_245_translated_title(md, entry, value):
    if value is None:
        return

    md.setdefault("additionalTitles", []).append(
        {"title": {"lang": "en", "value": value}, "titleType": "translatedTitle"}
    )


@matches("24630n", "24630p")
def transform_246_title_alternate(md, entry, val):
    _transform_title(md, entry, "alternativeTitle", val)


@matches("24633a")
def transform_24633a_subtitle(md, entry, val):
    _transform_title(md, entry, "subtitle", val)


@matches("24633b")
def transform_24633b_subtitle(md, entry, val):
    if val is None:
        return

    md.setdefault("additionalTitles", []).append(
        {"title": {"lang": "en", "value": val}, "titleType": "subtitle"}
    )


@matches("260__b")
def transform_260_publisher(md, entry, val):
    md.setdefault("publishers", []).append(val)


@matches("4900_a", "4900_v", paired=True)
def transform_490_series(md, entry, value):
    md.setdefault("series", []).append(
        make_dict("seriesTitle", value[0], "seriesVolume", value[1])
    )


@matches("520__a", "520__9", paired=True)
def transform_520_abstract(md, entry, value):
    try:
        md.setdefault("abstract", []).append(
            {"lang": get_alpha2_lang(value[1]), "value": value[0]}
        )
    except LookupError:
        md.setdefault("abstract", []).append(
            {
                "lang": value[1] or "cs",
                "value": value[0],
            }  # marshmallow will take care of that
        )


@matches("598__a")
def transform_598_note(md, entry, value):
    if value is None:
        return

    md.setdefault("notes", []).append(value)


@matches("65007a", "65007j", "650072", "650070", paired=True)
def transform_65007_subject(md, entry, value):
    transform_subject(md, value)


@matches("65017a", "65017j", "650172", "650170", paired=True)
def transform_65017_subject(md, entry, value):
    transform_subject(md, value)


@matches("650_7a", "650_7j", "650_72", "650_70", "650_77", paired=True)
def transform_650_7_subject(md, entry, value):
    transform_subject(md, value)


def transform_subject(md, value):
    if any(v is None for v in value):
        return

    purl = value[3] or ""
    val_url = (
        purl if purl.startswith("http://") or purl.startswith("https://") else None
    )
    class_code = value[4] if len(value) > 4 else None
    if not class_code and not (
        purl.startswith("http://") or purl.startswith("https://")
    ):
        class_code = purl

    md.setdefault("subjects", []).append(
        make_dict(
            "subjectScheme",
            value[2],
            "classificationCode",
            class_code,
            "valueURI",
            val_url,
            "subject",
            make_array(
                value[0],
                lambda: {"lang": "cs", "value": value[0]},
                value[1],
                lambda: {"lang": "en", "value": value[1]},
            ),
        )
    )


@matches("6530_a")
def transform_6530_en_keywords(md, entry, value):
    # splitnout take na carce
    for v in value.split("|"):
        v = v.strip()
        if not v:
            continue
        md.setdefault("subjects", []).append(
            {"subjectScheme": "keyword", "subject": [{"lang": "en", "value": v}]}
        )


@matches("653__a")
def transform_653_cs_keywords(md, entry, value):
    # splitnout take na carce
    for v in value.split("|"):
        v = v.strip()
        if not v:
            continue
        md.setdefault("subjects", []).append(
            {"subjectScheme": "keyword", "subject": [{"lang": "cs", "value": v}]}
        )


@matches("7112_a", "7112_c", "7112_d", "7112_g", paired=True)
def transform_7112_event(md, entry, value):
    event = {"eventNameOriginal": value[0]}

    alternate_name = value[3]
    if alternate_name:
        event["eventNameAlternate"] = [alternate_name]

    date = value[2]
    if date:
        event["eventDate"] = convert_to_date(date)

    place = value[1]
    if place:
        place = parse_place(place)
        if place:
            event["eventLocation"] = place

    return event


def parse_place(place):
    res = {}

    if place.lower() == "online":
        return {"place": place}

    if re.search(r"\(\-\)", place):
        # no country code, therefore no place
        return res

    place_array = place.strip().rsplit("(", 1)

    # matches multiple countries (2+) e.g. (CZ, SK, PL)
    multiple_countries_match = re.search(
        r"\(([a-zA-Z][a-zA-Z]+)(,\s*[a-zA-Z][a-zA-Z]+)+\)", place
    )
    if multiple_countries_match:
        country = multiple_countries_match.group(1).strip().upper()
    else:
        country = place_array[-1].replace(")", "").strip().upper()
        country = re.sub(r"\W", "", country)

    place = place_array[0].strip()
    if place:
        countries = vocabulary_cache.by_id("countries", "id")
        if country not in countries:
            raise KeyError(f"Bad country code {country}")
        res["place"] = place
        res["country"] = {"id": country}
    return res

@matches("720__a", "720__5", "720__6", paired=True, unique=True)
def transform_720_creator(md: Dict, entry: Dict, value: Tuple) -> None:
    if not value or not value[0] or value[0] == "et. al.":
        return

    name, affiliations, identifiers = value
    name_type, authority_identifiers, processed_affiliations = _process_person_info(
        name, affiliations, identifiers
    )

    creator = _create_person_dict(
        name, name_type, authority_identifiers, processed_affiliations
    )
    
    md.setdefault("creators", []).append(creator)

@matches("720__i", "720__e", "720__5", "720__6", paired=True, unique=True)
def transform_720_contributor(md: Dict, entry: Dict, value: Tuple) -> None:
    if not value or not value[0]:
        return

    name, role, affiliations, identifiers = value
    name_type, authority_identifiers, processed_affiliations = _process_person_info(
        name, affiliations, identifiers
    )

    contributor_types = vocabulary_cache.by_id("contributor-types", "id", "title")
    role_from_vocab = {"id": contributor_types["other"]["id"]}
    
    if role:
        for contributor_type in contributor_types.values():
            if role in (contributor_type["title"]["cs"], contributor_type["title"]["en"]):
                role_from_vocab["id"] = contributor_types[
                    contributor_type["title"]["en"]
                ]["id"]
                break

    contributor = _create_person_dict(
        name, name_type, authority_identifiers, processed_affiliations
    )
    contributor["contributorType"] = role_from_vocab

    md.setdefault("contributors", []).append(contributor)


@matches("7731_t", "7731_z", "7731_x", "7731_g", paired=True)
def transform_7731_related_item(md, entry, value):
    item_volume_issue = value[3]
    if item_volume_issue:
        item_volume_issue_parsed = parse_item_issue(item_volume_issue)
        if not item_volume_issue_parsed:
            item_volume_issue_parsed = {
                "itemIssue": item_volume_issue,
                "error": "Bad format",
            }
    else:
        item_volume_issue_parsed = {}

    identifiers = []
    if value[1]:
        parse_isbn(value[1], identifiers)

    if value[2]:
        parse_issn(value[2], identifiers)

    md.setdefault("relatedItems", []).append(
        {
            **make_dict("itemTitle", value[0], "itemPIDs", identifiers),
            **item_volume_issue_parsed,
        }
    )


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
    for vv in re.split("[,;]", value):
        vv.replace("(CZ)", "")
        vv.replace("(EN)", "")
        vv = vv.strip()
        if vv.lower().startswith("isbn:"):
            vv = vv[5:].strip()
        if vv.lower().startswith("isbn"):
            vv = vv[4:].strip()
        if vv.startswith("(") and vv.endswith(")"):
            vv = vv[1:-1]
        if not vv or vv == "N":
            continue
        identifiers.append(_create_identifier_object("ISBN", vv))


def parse_item_issue(text: str):
    if re.match(r"^\d+$|^\d+[-–]\d+$", text):
        # Item issues in the format issue/issueStart-issueEnd
        return {"itemIssue": text}

    if re.match(r"^\d+/\d+$", text):
        # Item issues with year in the format issue/year
        issue, year = text.split("/")
        return {"itemIssue": issue, "itemYear": year}

    number_match = re.match(r"^No\.\s+(\d+)$", text)
    if number_match:
        # Item issues with year in the format "No. issue"
        issue = number_match.groups()[0]
        return {
            "itemIssue": issue,
        }

    number_year_match = re.match(r"^No\.\s+(\d+),\s+(\d+)$", text)
    if number_year_match:
        # Item issues with year in the format "No. issue, year"
        issue, year = number_year_match.groups()
        return {"itemIssue": issue, "itemYear": year}

    dict_ = {
        "Roč. 22, č. 2 (2011)": {
            "itemVolume": "22",
            "itemIssue": "2",
            "itemYear": "2011",
        },
        "2008": {"itemYear": "2008"},
        "Roč. 19 (2013)": {"itemVolume": "19", "itemYear": "2013"},
        "Roč. 2016": {"itemYear": "2016"},
        "roč. 2, č. 2, s. 76-86": {
            "itemVolume": "2",
            "itemIssue": "2",
            "itemStartPage": "76",
            "itemEndPage": "86",
        },
        "roč. 7 (2021), 23": {"itemVolume": "7", "itemYear": "2021", "itemIssue": "23"},
        "ročník XXXII , č. 6 (2022)": {
            "itemVolume": "32",
            "itemIssue": "6",
            "itemYear": "2022",
        },
        "Únor 2022": {"itemIssue": "2", "itemYear": "2022"},
        "ročník 72, číslo 7–8/2022": {
            "itemVolume": "72",
            "itemIssue": "7–8",
            "itemYear": "2022",
        },
        "Vol. 19, Nos. 1/2/3": {"itemVolume": "19", "itemIssue": "1-3"},
        "Ročník 22, číslo 4": {"itemVolume": "22", "itemIssue": "4"},
        "č. 3/2018": {"itemIssue": "3", "itemYear": "2018"},
        "Únor": {"itemIssue": "2"},
        "číslo 4": {"itemIssue": "4"},
        "Nos 1/2/3": {"itemIssue": "1-3"},
        "číslo 7-8/2022": {"itemIssue": "7-8", "itemYear": "2022"},
        "č. 6 (2022)": {"itemIssue": "6", "itemYear": "2022"},
    }
    return dict_.get(text)


@matches("85640u", "85640z", paired=True)
def transform_85640_original_record_url(md, entry, value):
    if value[1] == "Odkaz na původní záznam":
        md["originalRecord"] = value[0]
        if "hdl.handle.net" in value[0]:
            md.setdefault("objectIdentifiers", []).append(
                _create_identifier_object("Handle", value[0])
            )


@matches("85642u")
def transform_85642_external_location(md, entry, value):
    md["externalLocation"] = {"externalLocationURL": value}


@matches("970__a")
def transform_970_catalogue_sysno(md, entry, value):
    md.setdefault("systemIdentifiers", []).append(
        _create_identifier_object("catalogueSysNo", value)
    )


@matches("980__a")
def transform_980_resource_type(md, entry, value):
    if value == "metodiky" and "336__" not in entry.entry:
        value = "methodology-without-certification"
    else:
        value = {
            "tematicke_sborniky": "book",
            "monografie": "book",
            "preprinty": "article",
            "prispevky_z_konference": "paper",
            "sborniky": "proceeding",
            "programy": "programme",
            "postery": "poster",
            "bakalarske_prace": "bachelor",
            "diplomove_prace": "master",
            "rigorozni_prace": "rigorous",
            "disertacni_prace": "doctoral",
            "habilitacni_prace": "post-doctoral",
            "metodiky": "certified-methodology",
            "vyrocni_zpravy": "annual",
            "vyzkumne_zpravy": "research",
            "technicke_zpravy": "research",
            "zaverecne_zpravy_z_projektu": "project",
            "prubezne_zpravy_z_projektu": "project",
            "grantove_zpravy": "project",
            "statisticke_zpravy": "statistical-or-status",
            "zpravy_o_stavu": "statistical-or-status",
            "zpravy_z_pruzkumu": "field",
            "cestovni_zpravy": "business-trip",
            "tiskove_zpravy": "press-release",
            "firemni_tisk": "trade-literature",
            "katalogy_vyrobku": "trade-literature",
            "letaky": "trade-literature",
            "vestniky": "trade-literature",
            "brozury": "trade-literature",
            "analyzy": "studies-and-analyses",
            "studie": "studies-and-analyses",
            "referaty": "educational-material",
            "katalogy_vystav": "exhibition-catalogue-or-guide",
            "pruvodce_expozici": "exhibition-catalogue-or-guide",
            "'pruvodce_expozici": "exhibition-catalogue-or-guide",
        }.get(value.strip(), "other")

    resource_type = vocabulary_cache.by_id("resource-types")[value]

    md["resourceType"] = resource_type


@matches("996__a", "996__b", "996__9", paired=True)
def transform_996_accessibility(md, entry, value):
    md["accessibility"] = make_array(
        value[0],
        {"lang": "cs", "value": value[0]},
        value[1],
        {"lang": "en", "value": value[1]},
    )
    if value[0]:
        md["accessRights"] = get_access_rights(text=value[0])
    else:
        md["accessRights"] = get_access_rights(slug=value[2] or "c_abf2")


access_right_dict = {
    "0": "c_14cb",  # pouze metadata
    "1": "c_abf2",  # open
    "2": "c_16ec",  # omezeny
}


def get_access_rights(text=None, slug=None):
    if not slug:
        sentence_dict = {
            "Dokument je dostupný v repozitáři Akademie věd.": "1",
            "Dokumenty jsou dostupné v systému NK ČR.": "1",
            "Plný text je dostupný v Digitální knihovně VUT.": "1",
            "Dostupné v digitálním repozitáři VŠE.": "1",
            "Plný text je dostupný v digitálním repozitáři JČU.": "1",
            "Dostupné v digitálním repozitáři UK.": "1",
            "Dostupné v digitálním repozitáři Mendelovy univerzity.": "1",
            "Dostupné v repozitáři ČZU.": "1",
            "Dostupné registrovaným uživatelům v digitálním repozitáři AMU.": "2",
            "Dokument je dostupný v NLK. Dokument je dostupný též v digitální formě v Digitální "
            "knihovně NLK. Přístup může být vázán na prohlížení z počítačů NLK.": "2",
            "Dostupné v digitálním repozitáři UK (pouze z IP adres univerzity).": "2",
            "Text práce je neveřejný, pro více informací kontaktujte osobu uvedenou v repozitáři "
            "Mendelovy univerzity.": "2",
            "Dokument je dostupný na vyžádání prostřednictvím repozitáře Akademie věd.": "2",
            "Dokument je dostupný v příslušném ústavu Akademie věd ČR.": "0",
            "Dokument je po domluvě dostupný v budově Ministerstva životního prostředí.": "0",
            "Plný text není k dispozici.": "0",
            "Dokument je dostupný v NLK.": "0",
            "Dokument je po domluvě dostupný v budově <a "
            'href="http://www.mzp.cz/__C125717D00521D29.nsf/index.html" '
            'target="_blank">Ministerstva životního prostředí</a>.': "0",
            "Dostupné registrovaným uživatelům v knihovně Mendelovy univerzity v Brně.": "2",
            "Dostupné registrovaným uživatelům v repozitáři ČZU.": "2",
            "Dokument je dostupný na externích webových stránkách.": "0",
        }
        slug = sentence_dict.get(text, "0")
    slug = access_right_dict.get(slug, slug)
    return {"id": slug}


@matches("999C1a", "999C1b")
def transform_999C1_funding_reference(md, entry, val):
    if val is None:
        return
    
    project_id, funder = val
    if funder and project_id:
        md.setdefault("fundingReferences", []).append(
            make_dict("projectID", project_id, "funder", funder)
        )

@matches("04107a", "04107b")
def transform_04107_language(md, entry, value):
    if value:
        try:
            md.setdefault("languages", []).append({"id": get_alpha2_lang(value)})
        except LookupError:
            raise Exception(f"Bad language {value} - no alpha2 equivalent")


@matches("336__a")
def transform_336_certifikovana_metodika(md, entry, value):
    md["resourceType"] = vocabulary_cache.by_id("resource-types")[
        "certified-methodology"
    ]


@matches("540__a", "540__9", paired=True)
def transform_540_rights(md, entry, value):
    if value[1] != "cze":
        return
    rights = value[0]
    rights = parse_rights(value[0])
    if rights:
        md.setdefault("rights", {}).update(rights)


rights_dict = {
    # 'Dílo je chráněno podle autorského zákona č. 121/2000 Sb.': 'copyright', # vyhozeno, protoze uz neni ve slovniku
    # 'Text je chráněný podle autorského zákona č. 121/2000 Sb.': 'copyright', # vyhozeno, protoze uz neni ve slovniku
    "Licence Creative Commons Uveďte autora 3.0 Česko": "3-BY-CZ",
    "Licence Creative Commons Uveďte autora-Neužívejte dílo komerčně 3.0 Česko": "3-BY-NC-CZ",
    "Licence Creative Commons Uveďte autora-Neužívejte dílo komerčně-Nezasahujte do díla 3.0 "
    "Česko": "3-BY-NC-ND-CZ",
    "Licence Creative Commons Uveďte autora-Neužívejte dílo komerčně-Zachovejte licenci 3.0 "
    "Česko": "3-BY-NC-SA-CZ",
    "Licence Creative Commons Uveďte autora-Nezasahujte do díla 3.0 Česko": "3-BY-ND-CZ",
    "Licence Creative Commons Uveďte autora-Zachovejte licenci 3.0 Česko": "3-BY-SA-CZ",
    "Licence Creative Commons Uveďte původ 4.0": "4-BY",
    "Licence Creative Commons Uveďte původ-Neužívejte komerčně-Nezpracovávejte 4.0": "4-BY-NC-ND",
    "Licence Creative Commons Uveďte původ-Neužívejte komerčně-Zachovejte licenci 4.0": "4-BY-NC-SA",
    "Licence Creative Commons Uveďte původ-Zachovejte licenci 4.0": "4-BY-SA",
    "LicenceCreativeCommonsUveďtepůvod-Nezpracovávejte4.0": "4-BY-ND",
    "LicenceCreativeCommonsUveďtepůvod-Neužívejtekomerčně4.0": "4-BY-NC"
}


def parse_rights(text):
    right = rights_dict.get(text)
    if not right:
        return None
    return vocabulary_cache.by_id("rights", "id")[right]


def transform_oai_identifier(md, entry):
    md.setdefault("systemIdentifiers", []).append(
        _create_identifier_object("nuslOAI", entry.context["oai"]["identifier"])
    )


@matches("502__c")
def transform_502_degree_grantor(md, entry, value):
    institution = vocabulary_cache.get_institution(value)
    if institution:
        md.setdefault("thesis", {}).setdefault("degreeGrantors", []).append(institution)


@matches("7102_a", "7102_b", "7102_g", "7102_9", paired=True)
def transform_7102_degree_grantor(md, entry, value):
    if value[3] != "cze":
        return
    institution = []
    if value[0]:
        institution.append(value[0])
    if value[1]:
        if value[1].startswith("Program "):
            md.setdefault("thesis", {}).setdefault("studyFields", []).extend(
                value[1][len("Program ") :]
            )
        else:
            institution.append(value[1])
    if value[2]:
        institution.append(value[2])
    if institution:
        institution = vocabulary_cache.get_institution(", ".join(institution))
        if institution:
            md.setdefault("thesis", {}).setdefault("degreeGrantors", []).append(
                institution
            )


@matches("586__a")
def transform_586_defended(md, entry, value):
    if value == "obhájeno":
        md.setdefault("thesis", {})["defended"] = True


@matches("656_7a")
def transform_656_study_field(md, entry, value):
    value = [x.strip() for x in value.split("/")]
    value = [x for x in value if x]
    md.setdefault("thesis", {}).setdefault("studyFields", []).extend(value)


@matches("998__a")
def transform_998_collection(md, entry, value):
    from invenio_communities.proxies import current_communities
    from invenio_access.permissions import system_identity

    # Note:
    # Keys are legacy NUSL ids that are no longer used, values are their new values.
    # Because there are still records with legacy NUSL ids, this mapping is needed.
    legacy_nusl_id_mapping_to_slug = {
        "centrum_pro_vyzkum_verejneho_mineni": "sociologicky-ustav",
        "katedra_socialni_geografie_prf": "univerzita-karlova-v-praze",
        "ustav_pro_dejiny_umeni_ff_uk": "univerzita-karlova-v-praze",
        "farmakologicky_ustav": "ustav-experimentalni-mediciny",
        "ustav_fyzikalniho_inzenyrstvi": "ustav-termomechaniky",
        "ustav_pro_elektrotechniku": "ustav-termomechaniky"
    }
    nusl_id = legacy_nusl_id_mapping_to_slug.get(value, value)
    slug = nusl_id.replace("_", "-")

    slug_filter = dsl.Q("term", **{ "slug": slug })
    results = current_communities.service.search(system_identity, extra_filter=slug_filter)
    if not results:
        raise ValueError(f"{value} is not a valid slug for any community.")
    community = list(results)[0]
    entry.transformed.setdefault("parent", {}).setdefault("communities", {})["default"] = community["id"]


@matches("502__a")
def transform_502_date_defended(md, entry, value):
    date_defended = convert_to_date(value)
    md.setdefault("thesis", {})["dateDefended"] = date_defended

    date_issued = md.setdefault("dateIssued", "")
    if not date_issued:
        md["dateIssued"] = date_defended


class VocabularyCache:
    def by_id(self, vocabulary_type, *fields):
        if not fields:
            fields = ["id"]
        key = f"vocabulary-cache-{vocabulary_type}"
        ret = current_cache.get(key)
        if ret:
            return ret

        from invenio_vocabularies.proxies import current_service
        from invenio_access.permissions import system_identity

        try:
            vocabulary_data = current_service.scan(
                system_identity,
                extra_filter=dsl.Q("term", type__id=vocabulary_type),
            )
        except sqlalchemy.exc.NoResultFound:
            raise KeyError(f"Vocabulary '{vocabulary_type}' has not been found")
        ret = {
            x["id"]: {k: v for k, v in x.items() if k in fields}
            for x in list(vocabulary_data)
        }
        log.info(
            f"Caching {vocabulary_type} for {DEFAULT_VOCABULARY_CACHE_TTL} seconds"
        )
        current_cache.set(key, ret, timeout=DEFAULT_VOCABULARY_CACHE_TTL)
        return ret

    def get_institution(self, inst):
        inst = (inst or "").strip()
        if not inst:
            return None
        cache_key = f"institution-vocabulary-lookup-{inst}"
        resolved = current_cache.get(cache_key)
        if resolved:
            return resolved

        # Step 1: split the institution on dots or commas and generate query to institutions vocabulary
        inst_pieces = re.split("([.,'])", inst)
        # Step 2: get all candidates
        candidate_strings = []
        for start in range(0, len(inst_pieces)):
            if inst_pieces[start] in (".", ",", "", "'"):
                continue
            for end in range(start, len(inst_pieces)):
                if inst_pieces[end] in (".", ",", "", "'"):
                    continue
                candidate_strings.append("".join(inst_pieces[start : end + 1]).strip())
        if not candidate_strings:
            raise KeyError(
                f"Can not transform institution name {inst} - no letters found"
            )
        # Step 3: use service to find candidates
        q = " OR ".join(
            f'hierarchy.title.cs: "{lucene_escape(x)}"^2 OR nonpreferredLabels.cs: "{lucene_escape(x)}"'
            for x in candidate_strings
        )
        from invenio_vocabularies.proxies import current_service
        from invenio_access.permissions import system_identity

        resp = current_service.search(
            system_identity, type="institutions", params={"q": q}
        )
        candidates = {r["id"]: r for r in list(resp)}
        if not candidates:
            return None
        # get all ancestors
        missing = set()
        for c in candidates.values():
            for anc in c["hierarchy"]["ancestors"]:
                if anc not in candidates:
                    missing.add(anc)
        with_ancestors = {**candidates}
        if missing:
            resp = current_service.read_many(
                system_identity, type="institutions", ids=list(missing)
            )
            for r in list(resp):
                with_ancestors[r["id"]] = r

        scored_candidates = [
            (
                self._get_institution_score(inst, c, with_ancestors),
                c,
            )
            for c in candidates.values()
        ]
        scored_candidates.sort(key=lambda x: -x[0])
        ret = None
        if scored_candidates[0][0] > 0.8:
            ret = {"id": scored_candidates[0][1]["id"]}

        current_cache.set(cache_key, ret, timeout=DEFAULT_VOCABULARY_CACHE_TTL)
        return ret

    def by_ico(self, ico):
        from invenio_vocabularies.proxies import current_service
        from invenio_access.permissions import system_identity

        q = f'props.ICO:"{ico}"'
        resp = current_service.search(
            system_identity, type="institutions", params={"q": q}
        )
        return "Organizational" if len(list(resp)) > 0 else None

    def _get_institution_score(self, inst_string, candidate, ancestors):
        def powerset(iterable):
            s = list(iterable)
            return list(
                itertools.chain.from_iterable(
                    itertools.combinations(s, r) for r in range(len(s) + 1)
                )
            )

        ancestor_combinations = powerset(candidate["hierarchy"]["ancestors"])
        best_score = -1
        for c in ancestor_combinations:
            score = self._get_institution_score_ids(
                inst_string, ancestors, [candidate["id"], *c]
            )
            if score > best_score:
                best_score = score
        return best_score

    def _get_institution_score_ids(self, inst_string, ancestors, ids):
        inst_parts = set(x.lower() for x in re.split(r"\W", inst_string) if x)
        matches = []
        for c_id in ids:
            c = ancestors[c_id]
            c_matches = []
            title_parts = set(
                x.lower()
                for x in re.split(r"\W", c["title"].get("cs") or c["title"].get("en"))
                if x
            )
            c_matches.append(self._match_strings(inst_parts, title_parts))
            for np in c.get("nonpreferredLabels", []):
                if "cs" in np:
                    np_parts = set(x.lower() for x in re.split(r"\W", np["cs"]) if x)
                    c_matches.append(self._match_strings(inst_parts, np_parts))
            c_matches.sort(key=lambda x: (-x[0], len(x[2])))
            matches.append(c_matches[0])
        alternative_parts = set()
        for m in matches:
            alternative_parts.update(m[2])
        score1, _, _ = self._match_strings(inst_parts, alternative_parts)
        score2, _, _ = self._match_strings(alternative_parts, inst_parts)
        return min(score1, score2)

    def _match_strings(self, tested_parts, alternative_parts):
        if not tested_parts or not alternative_parts:
            return -1, set(), set()

        distances = []
        matched_tested = set()
        for tested_part in tested_parts:
            dist = 0
            match = None
            for alternative_part in alternative_parts:
                test_dist = Levenshtein.jaro_winkler(tested_part, alternative_part)
                if test_dist > 0.9 and test_dist > dist:
                    dist = test_dist
                    match = alternative_part
            if match:
                matched_tested.add(tested_part)
            distances.append(dist)
        return sum(distances) / len(distances), matched_tested, alternative_parts


lucene_escape_chars = {
    "+",
    "-",
    "&",
    "|",
    "!",
    "(",
    ")",
    "{",
    "}",
    "[",
    "]",
    "^",
    '"',
    "~",
    "*",
    "?",
    ":",
    "\\",
}


def lucene_escape(str):
    return "".join(f"\\{x}" if x in lucene_escape_chars else x for x in str)


def convert_to_date(value):
    if not value:
        return value
    if value.startswith("["):
        value = value[1:]
    if value.endswith("]"):
        value = value[:-1]
    value = value.replace(" 00:00:00.0", "")
    return value


vocabulary_cache = VocabularyCache()


def resolve_name_type(value, ico=None):
    """
    Based on the given value of creator or contributor, applies heuristic rules
    to determine the `nameType`. When none of the rules applied, default is `Personal`.

    Returns either `Organizational` or `Personal`.
    """
    value = value.strip()
    try:
        inst = vocabulary_cache.get_institution(value)
    except KeyError:
        return None

    if ico:
        try:
            inst = vocabulary_cache.by_ico(ico)
        except KeyError:
            return None

    if inst:
        return "Organizational"

    return "Personal"

def _process_person_info(
    name: str,
    affiliations: Optional[Union[str, List[str]]] = None,
    identifiers: Optional[Union[str, List[str]]] = None
) -> Tuple[str, List[Dict], List[Dict]]:
    """Process common person information for both creators and contributors."""
    name_type = ""
    authority_identifiers = []
    processed_affiliations = []

    # Process affiliations
    if affiliations:
        affiliations = [affiliations] if isinstance(affiliations, str) else [aff for aff in affiliations if aff]
        if any("ror" in aff for aff in affiliations):
            name_type = "Personal"
        processed_affiliations = _process_affiliations(affiliations)

    # Process identifiers
    if identifiers:
        identifiers = [identifiers] if isinstance(identifiers, str) else [idf for idf in identifiers if idf]
        if any("ror" in idf for idf in identifiers):
            name_type = "Organizational"
        authority_identifiers = [
            _create_identifier_object(*_parse_identifier(idf))
            for idf in identifiers
            if idf
        ]

    # Determine name type if not already set
    if not name_type:
        ico_idf = [idf for idf in identifiers if "ICO" in idf] if identifiers else []
        if ico_idf:
            name_type = resolve_name_type(name, ico_idf[0].split(": ")[1])
        else:
            name_type = resolve_name_type(name)

    return name_type, authority_identifiers, processed_affiliations

def _create_person_dict(
    name: str,
    name_type: str,
    authority_identifiers: List[Dict],
    affiliations: Optional[List[Dict]] = None
) -> Dict:
    """Create a base dictionary for a person (creator or contributor)."""
    person_dict = {
        "fullName": name,
        "nameType": name_type,
        "authorityIdentifiers": authority_identifiers
    }

    if name_type == "Personal":
        given_name, family_name = _parse_personal_name(name)
        person_dict.update({
            "givenName": given_name,
            "familyName": family_name
        })
        
        if affiliations:
            person_dict["affiliations"] = affiliations

    return person_dict

def _parse_identifier(identifier: str) -> Tuple[str, str]:
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

def _create_identifier_object(scheme: str, identifier: str) -> Dict[str, str]:
    return {
        "scheme": scheme,
        "identifier": identifier
    }

def _process_affiliations(affiliations: List[str]) -> List[Dict[str, str]]:
    from invenio_vocabularies.proxies import current_service
    from invenio_access.permissions import system_identity

    def _prepare_affiliation_query(affiliation: str):
        if "ror" in affiliation:
            escaped_url = lucene_escape(affiliation)
            return f'relatedURI.ROR:"{escaped_url}"'
        elif "ICO" in affiliation:
            return f'props.ICO:"{affiliation.split(": ")[-1]}"'
        else:
            escaped_name = lucene_escape(affiliation)
            candidates = [
                "props.acronym",
                "title.en",
                "title.cs",
                "nonpreferredLabels.cs",
                "nonpreferredLabels.en"
            ]
            return " OR ".join([f'{candidate}:"{escaped_name}"' for candidate in candidates])

    vocabulary_affiliations = []
    for affiliation in affiliations:
        query = _prepare_affiliation_query(affiliation)
        resp = current_service.search(
            system_identity, type="institutions", params={"q": query}
        )

        try:
            vocabulary_affiliations.append(list(resp)[0])
        except IndexError:
            raise ValueError(f"Affiliation: '{affiliation}' not found in the institution vocabulary.")

    return vocabulary_affiliations

def _parse_personal_name(name: str) -> Tuple[str, str]:
    names = name.split(",")
    family_name = names[0].strip()
    given_name = "".join(names[1:]).strip(",").strip()
    return given_name, family_name

def _transform_title(md, entry, titleType, val):
    if val is None:
        return

    try:
        lang_entry = entry.entry.get("04107a")
        if isinstance(lang_entry, list):
            lang_entry = list(filter(lambda x: x is not None, lang_entry))
            lang_entry = None if not lang_entry else lang_entry[0]

        lang = get_alpha2_lang(lang_entry)
        md.setdefault("additionalTitles", []).append(
            {"title": {"lang": lang, "value": val}, "titleType": titleType}
        )
    except LookupError:
        # append it with the original language, marshmallow will take care of that
        md.setdefault("additionalTitles", []).append(
            {
                "title": {"lang": lang_entry, "value": val},
                "titleType": titleType,
            }
        )