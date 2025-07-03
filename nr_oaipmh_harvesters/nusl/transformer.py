import itertools
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

import Levenshtein
import pycountry
import sqlalchemy
from invenio_cache.proxies import current_cache
from invenio_search.engine import dsl
from oarepo_oaipmh_harvester.transformers.rule import (
    OAIRuleTransformer,
    deduplicate,
    ignore,
    make_array,
    make_dict,
    matches,
    matches_grouped,
)
from oarepo_runtime.datastreams.types import StreamEntry, StreamEntryFile

from nr_oaipmh_harvesters.nusl.temp_institutions import TEMP_INSTITUTIONS

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


@matches("001")
def transform_001_control_number(md, entry, value):
    md.setdefault("systemIdentifiers", []).append(
        _create_identifier_object("nusl", "http://www.nusl.cz/ntk/nusl-" + value)
    )


@matches("020__a")
def transform_020_isbn(md, entry, value):
    identifiers = []
    parse_isbn(value, identifiers)

    md.setdefault("objectIdentifiers", []).append(identifiers[0])


@matches("022__a")
def transform_022_issn(md, entry, value):
    identifiers = []
    parse_issn(value, identifiers)

    md.setdefault("objectIdentifiers", []).append(identifiers[0])


@matches("035__a")
def transform_035_original_record_oai(md, entry, value):
    md.setdefault("systemIdentifiers", []).append(
        _create_identifier_object("originalRecordOAI", value)
    )


@matches("046__j")
def transform_046_date_modified(md, entry, value):
    md["dateModified"] = convert_to_date(value)


@matches("046__k")
def transform_046_date_issued(md, entry, value):
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
    md["title"] = value


@matches("24500b")
def transform_245_translated_title(md, entry, value):
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
    if all(not v for v in value):
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

    md.setdefault("events", []).append(event)


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


@matches_grouped("720__a", "720__5", "720__6", unique=True, group=["720__5", "720__6"])
def transform_720_creator(md: Dict, entry: Dict, value: Tuple) -> None:
    if not value[0] or value[0] == "et. al.":
        return

    name, affiliations, identifiers = value

    name_type = None
    processed_affiliations = []
    authority_identifiers = []

    if affiliations:
        affiliations = [affiliations] if isinstance(affiliations, str) else affiliations
        affiliations = [aff for aff in affiliations if aff]
        processed_affiliations = _process_affiliations_temp(affiliations)
    if identifiers:
        identifiers = [identifiers] if isinstance(identifiers, str) else identifiers
        identifiers = [idf for idf in identifiers if idf]
        authority_identifiers = [
            _create_identifier_object(*_parse_identifier(idf))
            for idf in identifiers
            if idf
        ]

    ror = [] if not identifiers else [idf for idf in identifiers if "ror" in idf]
    ico = (
        [] if not identifiers else [idf.split(" ")[-1] for idf in identifiers if "ico" in idf.lower()]
    )
    institution_was_found, institution = _find_institution_in_temp(
        name, None if not ror else ror[0], None if not ico else ico[0]
    )
    if institution_was_found:
        name_type = "organizational"
        name = institution["name"]
    elif not ror:
        creatibutor_was_found, _ = _find_creatibutor(authority_identifiers)
        if creatibutor_was_found:
            name_type = "personal"

    if not name_type and identifiers:
        raise ValueError(
            f"Can not automatically decide 'personal'/'organizational': {value}. Add to vocabulary then."
        )

    if not name_type and not identifiers:
        # Should be marked as deprecated and inserted into its particular vocabulary.
        # Currently we do not have tags in vocabularies so just add without insertion.
        log.warning(f"{value[0]} marked as personal")
        name_type = "personal"

    creator = {
        "affiliations": processed_affiliations,
        "person_or_org": {
            "name": name,
            "type": name_type,
            "identifiers": authority_identifiers,
        },
    }
    if name_type == "personal":
        given_name, family_name = _parse_personal_name(name)
        creator["person_or_org"].update(
            {
                "given_name": given_name,
                "family_name": family_name,
            }
        )

    md.setdefault("creators", []).append(creator)


@matches_grouped(
    "720__i", "720__e", "720__5", "720__6", unique=True, group=["720__5", "720__6"]
)
def transform_720_contributor(md: Dict, entry: Dict, value: Tuple) -> None:
    if not value[0]:
        return

    name, role, affiliations, identifiers = value

    name_type = None
    processed_affiliations = []
    authority_identifiers = []

    contributor_types = vocabulary_cache.by_id("contributor-types", "id", "title")
    role_from_vocab = {"id": contributor_types["other"]["id"]}

    if role:
        for contributor_type in contributor_types.values():
            if role in (
                contributor_type["title"]["cs"],
                contributor_type["title"]["en"],
            ):
                role_from_vocab["id"] = contributor_types[
                    contributor_type["title"]["en"]
                ]["id"]
                break

    if affiliations:
        affiliations = [affiliations] if isinstance(affiliations, str) else affiliations
        processed_affiliations = _process_affiliations_temp(affiliations)
    if identifiers:
        identifiers = [identifiers] if isinstance(identifiers, str) else identifiers
        authority_identifiers = [
            _create_identifier_object(*_parse_identifier(idf))
            for idf in identifiers
            if idf
        ]

    ror = [] if not identifiers else [idf for idf in identifiers if "ror" in idf]
    ico = (
        [] if not identifiers else [idf.split(" ")[-1] for idf in identifiers if "ico" in idf.lower()]
    )
    institution_was_found, institution = _find_institution_in_temp(
        name, None if not ror else ror[0], None if not ico else ico[0]
    )
    if institution_was_found:
        name_type = "organizational"
        name = institution["name"]
    elif not ror:
        creatibutor_was_found, _ = _find_creatibutor(authority_identifiers)
        if creatibutor_was_found:
            name_type = "personal"

    if not name_type and identifiers:
        raise ValueError(
            f"Can not automatically decide 'personal'/'organizational': {value}. Add to vocabulary then."
        )

    if not name_type and not identifiers:
        # Should be marked as deprecated and inserted into its particular vocabulary.
        # Currently we do not have tags in vocabularies so just add without insertion.
        log.warning(f"{value[0]} marked as personal")
        name_type = "personal"

    contributor = {
        "role": role_from_vocab,
        "affiliations": processed_affiliations,
        "person_or_org": {
            "name": name,
            "type": name_type,
            "identifiers": authority_identifiers,
        },
    }

    if name_type == "personal":
        given_name, family_name = _parse_personal_name(name)
        contributor["person_or_org"].update(
            {
                "given_name": given_name,
                "family_name": family_name,
            }
        )

    md.setdefault("contributors", []).append(contributor)


@matches("7731_e", "7731_f", "7731_g", "7731_z", "7731_t", "7731_x", paired=True)
def transform_7731_related_item(md, entry, value):
    item_year, item_volume, item_issue, item_pids_isbn, item_title, item_pids_issn = (
        value
    )

    parsed = {
        k: v
        for k, v in {
            "itemYear": item_year,
            "itemVolume": item_volume,
            "itemIssue": item_issue,
        }.items()
        if v
    }

    identifiers = []
    if item_pids_isbn:
        parse_isbn(item_pids_isbn, identifiers)
    if item_pids_issn:
        parse_issn(item_pids_issn, identifiers)

    md.setdefault("relatedItems", []).append(
        {
            **make_dict("itemTitle", item_title, "itemPIDs", identifiers),
            **parsed,
        }
    )
    print()


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
        isbn = isbn.strip()
        isbn = isbn.lower()
        isbn = re.sub(r"\s*\([^)]*\)", "", isbn)
        isbn = isbn.removeprefix("isbn:").removeprefix("isbn")
        isbn = isbn.strip()

        if isbn and isbn != "n":
            identifiers.append(_create_identifier_object("ISBN", isbn))


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
            "preprinty": "submitted-version",
            "postprinty": "accepted-version",
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


@matches("999C1a", "999C1b", paired=True)
def transform_999C1_funding_reference(md, entry, val):
    project_id, funder = val
    if project_id:
        from invenio_access.permissions import system_identity
        from invenio_vocabularies.proxies import current_service

        matched_award = None
        try:
            resp = current_service.search(
                system_identity,
                type="awards",
                extra_filter=dsl.Q("term", number=project_id),
            )
            matched_award = list(resp)[0]
        except Exception as e:
            if not funder:
                raise KeyError(f"Project ID: '{project_id}' has not been found") from e

        award = {}
        if matched_award:
            for field_in_award_datatype in [
                "id",
                "title",
                "number",
                "acronym",
                "program",
                "subjects",
                "organizations",
            ]:
                if field_in_award_datatype in matched_award:
                    award[field_in_award_datatype] = matched_award[
                        field_in_award_datatype
                    ]

        if not award:
            new_funder = {"funder": {"name": funder}}
        else:
            new_funder = {
                "award": award,
                "funder": {
                    "id": matched_award["funder"]["id"],
                    "name": matched_award["funder"]["name"],
                },
            }

        md.setdefault("funders", []).append(new_funder)


@matches("04107a", "04107b")
def transform_04107_language(md, entry, value):
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
    "Licence Creative Commons Uveďte autora 3.0 Česko": "CC-BY-CZ-3.0",
    "Licence Creative Commons Uveďte autora-Neužívejte dílo komerčně 3.0 Česko": "CC-BY-NC-CZ-3.0",
    "Licence Creative Commons Uveďte autora-Neužívejte dílo komerčně-Nezasahujte do díla 3.0 Česko": "CC-BY-NC-ND-CZ-3.0",
    "Licence Creative Commons Uveďte autora-Neužívejte dílo komerčně-Zachovejte licenci 3.0 Česko": "CC-BY-NC-SA-CZ-3.0",
    "Licence Creative Commons Uveďte autora-Nezasahujte do díla 3.0 Česko": "CC-BY-ND-CZ-3.0",
    "Licence Creative Commons Uveďte autora-Zachovejte licenci 3.0 Česko": "CC-BY-SA-CZ-3.0",
    "Licence Creative Commons Uveďte původ 4.0": "CC-BY-4.0",
    "Licence Creative Commons Uveďte původ-Neužívejte komerčně-Nezpracovávejte 4.0": "CC-BY-NC-ND-4.0",
    "Licence Creative Commons Uveďte původ-Neužívejte komerčně-Zachovejte licenci 4.0": "CC-BY-NC-SA-4.0",
    "Licence Creative Commons Uveďte původ-Zachovejte licenci 4.0": "CC-BY-SA-4.0",
    "Licence Creative Commons Uveďte původ-Nezpracovávejte 4.0": "CC-BY-ND-4.0",
    "Licence Creative Commons Uveďte původ-Neužívejte komerčně 4.0": "CC-BY-NC-4.0",
    "License: Creative Commons Attribution 4.0": "CC-BY-4.0",
    "License: Creative Commons Attribution-NoDerivs 3.0 Czech Republic": "CC-BY-ND-CZ-3.0",
    "License: Creative Commons Attribution-NoDerivs 4.0": "CC-BY-ND-4.0",
    "License: Creative Commons Attribution-NonCommercial 3.0 Czech Republic": "CC-BY-NC-CZ-3.0",
    "License: Creative Commons Attribution-NonCommercial 4.0": "CC-BY-NC-4.0",
    "License: Creative Commons Attribution-NonCommercial-NoDerivs 3.0 Czech Republic": "CC-BY-NC-ND-CZ-3.0",
    "License: Creative Commons Attribution-NonCommercial-NoDerivs 4.0": "CC-BY-NC-ND-4.0",
    "License: Creative Commons Attribution-NonCommercial-ShareAlike 3.0 Czech Republic": "CC-BY-NC-SA-CZ-3.0",
    "License: Creative Commons Attribution-NonCommercial-ShareAlike 4.0": "CC-BY-NC-SA-4.0",
    "License: Creative Commons Attribution-ShareAlike 3.0 Czech Republic": "CC-BY-SA-CZ-3.0",
    "License: Creative Commons Attribution-ShareAlike 4.0": "CC-BY-SA-4.0",
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
    degree_grantor = vocabulary_cache.get_institution(
        value, vocab_type="degree-grantors"
    )
    if degree_grantor:
        md.setdefault("thesis", {}).setdefault("degreeGrantors", []).append(
            degree_grantor
        )


@matches("7102_a", "7102_b", "7102_g", "7102_9", paired=True)
def transform_7102_degree_grantor(md, entry, value):
    if value[3] != "cze":
        return
    degree_grantor = []
    if value[0]:
        degree_grantor.append(value[0])
    if value[1]:
        if value[1].startswith("Program "):
            md.setdefault("thesis", {}).setdefault("studyFields", []).extend(
                value[1][len("Program ") :]
            )
        else:
            degree_grantor.append(value[1])
    if value[2]:
        degree_grantor.append(value[2])
    if degree_grantor:
        degree_grantor = vocabulary_cache.get_institution(
            ", ".join(degree_grantor), vocab_type="degree-grantors"
        )
        if degree_grantor:
            md.setdefault("thesis", {}).setdefault("degreeGrantors", []).append(
                degree_grantor
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


@matches("8564_u", "8564_z", "8564_y", paired=True)
def transform_856_attachments(md, entry, value):
    link, description, language_version = value
    filename = link.split("/")[-1]
    if filename is None:
        raise ValueError("File link is not present")

    if ".gif" in filename:
        return

    file_note = ""
    if description is not None:
        file_note = description
    if language_version is not None:
        file_note += f" ({language_version})"

    entry.files.append(
        StreamEntryFile(
            {"key": filename, "metadata": {"metadata": {"file_note": file_note}}}, link
        )
    )
    entry.transformed["files"]["enabled"] = True


@matches("998__a")
def transform_998_collection(md, entry, value):
    from invenio_access.permissions import system_identity
    from invenio_communities.proxies import current_communities

    nusl_id_to_slug_mapping = {
        "agritec": "7emz",
        "agrotest_fyto": "22g4",
        "agrovyzkum_rapotin": "zqh5",
        "akademie_muzickych_umeni_v_praze": "8gve",
        "akademie_vytvarnych_umeni": "nvcb",
        "archeologicky_ustav_brno": "tq0x",
        "archeologicky_ustav_praha": "fy00",
        "archip": "52fb",
        "archiv_doc_jiri_soucek": "qvhz",
        "archiv_ing_arch_jana_moucky": "4j8v",
        "arnika": "y0dw",
        "astronomicky_ustav": "vy9t",
        "biofyzikalni_ustav": "zpci",
        "biologicke_centrum": "n0xu",
        "biotechnologicky_ustav": "2qd9",
        "botanicky_ustav": "91ek",
        "cenia": "zn3n",
        "centrum_dopravniho_vyzkumu": "x6b0",
        "centrum_pro_dopravu_a_energetiku": "xmfn",
        "centrum_pro_regionalni_rozvoj": "3f7b",
        "centrum_pro_studium_vysokeho_skolstvi": "vvwy",
        "centrum_vyzkumu_globalni_zmeny": "j47p",
        "ceska_asociace_ergoterapeutu": "whmr",
        "ceska_asociace_paraplegiku": "exqr",
        "ceska_narodni_banka": "bnyi",
        "ceska_spolecnost_ornitologicka": "iq4i",
        "ceska_zemedelska_univerzita": "1tt2",
        "cesky_statisticky_urad": "badt",
        "cesnet": "ykbr",
        "clovek_v_tisni": "izxu",
        "crdm": "utzb",
        "cvut": "y9bq",
        "czwa": "cjyn",
        "ekodomov": "uh4x",
        "entomologicky_ustav": "zfpq",
        "etnologicky_ustav": "4jef",
        "evropske_hodnoty": "n2rk",
        "fairtrade_cz_sk": "zeea",
        "filosoficky_ustav": "720e",
        "fyzikalni_ustav": "yd66",
        "fyziologicky_ustav": "nrpj",
        "galerie_vytvarneho_umeni_v_ostrave": "t0nj",
        "gender_studies": "3fmh",
        "geofyzikalni_ustav": "z1e8",
        "geologicky_ustav": "50at",
        "gle": "6amm",
        "hestia": "azxq",
        "historicky_ustav": "y7gg",
        "hydrobiologicky_ustav": "v6jj",
        "chmelarsky_institut": "xrh6",
        "institut_umeni": "j9x8",
        "iuridicum_remedium": "ki0x",
        "jihoceska_univerzita_v_ceskych_budejovicich": "q6tw",
        "jihomoravske_muzeum_ve_znojme": "wk7f",
        "knihovna_av_cr": "mp7c",
        "masarykova_univerzita": "cujt",
        "masarykuv_ustav_a_archiv": "ku9r",
        "matematicky_ustav": "w6ed",
        "mendelova_univerzita_v_brne": "q46u",
        "mikrobiologicky_ustav": "egdf",
        "ministerstvo_spravedlnosti": "x7j7",
        "moravska_galerie": "z1em",
        "moravska_zemska_knihovna": "nm8z",
        "muzeum_brnenska": "21gf",
        "muzeum_skla_a_bizuterie": "9zy2",
        "muzeum_vychodnich_cech": "0zuv",
        "nacr": "w8y4",
        "nadace_promeny": "gj2a",
        "narodni_hrebcin_kladruby": "rtgy",
        "narodni_informacni_a_poradenske_stredisko_pro_kulturu": "ge7e",
        "narodni_knihovna": "vz34",
        "narodni_lekarska_knihovna": "a0hr",
        "narodni_muzeum": "19pv",
        "narodni_muzeum_v_prirode": "2nk4",
        "narodni_pamatkovy_ustav": "87p8",
        "narodni_technicka_knihovna": "fcq7",
        "narodni_technicke_muzeum": "7x0e",
        "narodni_zemedelske_muzeum": "2ixq",
        "narodohospodarsky_ustav": "99xq",
        "nfa": "gr8c",
        "nulk": "v0rv",
        "nuv": "wbhz",
        "orientalni_ustav": "r68a",
        "oseva": "5vxr",
        "ostravska_univerzita": "j61g",
        "pamatnik_narodniho_pisemnictvi": "4dz4",
        "parazitologicky_ustav": "4x3a",
        "parlamentni_institut": "mxan",
        "psychologicky_ustav": "umjz",
        "sdruzeni_pro_integraci_a_migraci": "d0y5",
        "severoceske_muzeum_v_liberci": "0d9d",
        "siriri": "xzdm",
        "slezska_univerzita_opava": "ymfu",
        "slezske_zemske_muzeum": "2a8j",
        "slovansky_ustav": "58f6",
        "centrum_pro_vyzkum_verejneho_mineni": "5pv8",
        "sociologicky_ustav": "5pv8",
        "surao": "5k78",
        "szpi": "azir",
        "technicke_muzeum_v_brne": "ugvw",
        "technologicke_centrum": "bb83",
        "uhk": "p80y",
        "ujep": "verw",
        "umeleckoprumyslove_museum": "ujww",
        "univerzita_karlova_v_praze": "8g23",
        "upce": "9f6r",
        "upol": "jivv",
        "urad_prumysloveho_vlastnictvi": "5wmr",
        "ustav_analyticke_chemie": "np2e",
        "ustav_anorganicke_chemie": "xaqc",
        "ustav_archeologicke_pamatkove_pece_severozapadnich_cech": "085p",
        "ustav_biologie_obratlovcu": "7a4m",
        "ustav_dejin_umeni": "3xe6",
        "ustav_experimentalni_botaniky": "ai17",
        "farmakologicky_ustav": "9pth",
        "ustav_experimentalni_mediciny": "9pth",
        "ustav_fotoniky_a_elektroniky": "dv07",
        "ustav_fyzikalni_chemie_j_heyrovskeho": "d6ab",
        "ustav_fyziky_atmosfery": "k7ki",
        "ustav_fyziky_materialu": "rj0h",
        "ustav_fyziky_plazmatu": "0kgr",
        "ustav_geoniky": "iftu",
        "ustav_chemickych_procesu": "2prw",
        "ustav_informatiky": "zfcp",
        "ustav_jaderne_fyziky": "pmyj",
        "ustav_makromolekularni_chemie": "4qfm",
        "ustav_molekularni_biologie_rostlin": "k4yf",
        "ustav_molekularni_genetiky": "12ey",
        "ustav_organicke_chemie_a_biochemie": "9rjk",
        "ustav_pristrojove_techniky": "g9t1",
        "ustav_pro_ceskou_literaturu": "5qqp",
        "ustav_pro_hydrodynamiku": "1t68",
        "ustav_pro_jazyk_cesky": "218m",
        "ustav_pro_soudobe_dejiny": "d6nj",
        "ustav_pro_studium_totalitnich_rezimu": "wwac",
        "ustav_pudni_biologie": "u7ee",
        "ustav_statu_a_prava": "4g9q",
        "ustav_struktury_a_mechaniky_hornin": "rzv2",
        "ustav_teoreticke_a_aplikovane_mechaniky": "yqr9",
        "ustav_teorie_informace_a_automatizace": "ygir",
        "ustav_fyzikalniho_inzenyrstvi": "e2dx",
        "ustav_pro_elektrotechniku": "e2dx",
        "ustav_termomechaniky": "e2dx",
        "ustav_zivocisne_fyziologie_a_genetiky": "m8yc",
        "vscht": "whdy",
        "vugtk": "8rg6",
        "vyzkumny_ustav_potravinarsky": "wvya",
        "vutbr": "5j61",
        "vuv_tgm": "t373",
        "vvud": "ieex",
        "vysoka_skola_ekonomicka_v_praze": "wzkj",
        "vysoka_skola_evropskych_a_regionalnich_studii": "ata0",
        "vysoka_skola_financni_a_spravni": "r3w4",
        "vyzkumny_ustav_bezpecnosti_prace": "km8v",
        "vyzkumny_ustav_lesniho_hospodarstvi_a_myslivosti": "i7tm",
        "vyzkumny_ustav_prace_a_socialnich_veci": "km8v",
        "vyzkumny_ustav_rostlinne_vyroby": "xnkt",
        "vyzkumny_ustav_silva_taroucy": "4eqq",
        "woodexpert": "0w0h",
        "zapadoceska_univerzita": "6f0m",
        "zapadoceske_muzeum_v_plzni": "efbe",
    }

    if value not in nusl_id_to_slug_mapping:
        raise ValueError(f"{value} is not a valid slug for any community.")

    slug_filter = dsl.Q("term", **{"slug": nusl_id_to_slug_mapping[value]})
    results = current_communities.service.search(
        system_identity, extra_filter=slug_filter
    )
    if not results:
        raise ValueError(f"{value} is not a valid slug for any community.")
    community = list(results)[0]
    entry.transformed.setdefault("parent", {}).setdefault("communities", {})[
        "default"
    ] = community["id"]


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

        from invenio_access.permissions import system_identity
        from invenio_vocabularies.proxies import current_service

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

    def get_institution(self, inst, vocab_type="institutions"):
        inst = (inst or "").strip()
        if not inst:
            return None
        cache_key = f"{vocab_type}-vocabulary-lookup-{inst}"
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
        from invenio_access.permissions import system_identity
        from invenio_vocabularies.proxies import current_service

        resp = current_service.search(system_identity, type=vocab_type, params={"q": q})
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
                system_identity, type=vocab_type, ids=list(missing)
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


def lucene_escape(str):
    return "".join(f"\\{x}" if x in LUCENE_ESCAPE_CHARS else x for x in str)


def convert_to_date(value):
    if not value:
        return value
    if value.startswith("["):
        value = value[1:]
    if value.endswith("]"):
        value = value[:-1]
    value = value.replace(" 00:00:00.0", "")
    return value.strip()


vocabulary_cache = VocabularyCache()


def _parse_identifier(identifier: str) -> Tuple[str, str]:
    normalized_identifier = identifier.lower()
    if "scopusid" in normalized_identifier:
        return "scopusId", identifier.split(": ")[1]
    elif "researcherid" in normalized_identifier:
        return "researcherId", identifier.split(": ")[1]
    elif "orcid" in normalized_identifier:
        return "orcid", identifier.split("/")[-1]
    elif "ico" in normalized_identifier:
        return "ico", identifier.split(": ")[1]
    elif "ror" in normalized_identifier:
        return "ror", identifier
    else:
        raise ValueError(f"Undefined scheme for the identifier: {identifier}")


def _create_identifier_object(scheme: str, identifier: str) -> Dict[str, str]:
    return {"scheme": scheme, "identifier": identifier}


def _process_affiliations(affiliations: List[str]) -> List[Dict[str, str]]:
    from invenio_access.permissions import system_identity
    from invenio_vocabularies.proxies import current_service

    def _prepare_affiliation_query(affiliation: str):
        if "ror" in affiliation.lower():
            escaped_url = lucene_escape(affiliation)
            return f'relatedURI.ROR:"{escaped_url}"'
        elif "ico" in affiliation.lower():
            return f'props.ICO:"{affiliation.split(": ")[-1]}"'
        else:
            escaped_name = lucene_escape(affiliation)
            candidates = [
                "props.acronym",
                *[f"title.{lang}" for lang in LANGUAGES_IN_INSTITUTIONS],
                *[f"nonpreferredLabels.{lang}" for lang in LANGUAGES_IN_INSTITUTIONS],
            ]
            return " OR ".join(
                [f'{candidate}:"{escaped_name}"' for candidate in candidates]
            )

    vocabulary_affiliations = []
    for affiliation in affiliations:
        query = _prepare_affiliation_query(affiliation)
        resp = current_service.search(
            system_identity, type="institutions", params={"q": query}
        )

        try:
            result = list(resp)[0]
            title = None
            if "cs" in result["title"]:
                title = result["title"]["cs"]
            else:
                title = list(result["title"].values())[0]
            if not title:
                raise ValueError(
                    f"Affiliation: '{affiliation}' does not have a valid title."
                )

            result = {"id": result["id"], "name": title}
            vocabulary_affiliations.append(result)
        except IndexError:
            raise ValueError(
                f"Affiliation: '{affiliation}' not found in the institution vocabulary."
            )

    return vocabulary_affiliations


def _process_affiliations_temp(affiliations: List[str]) -> List[Dict[str, str]]:
    vocabulary_affiliations = []
    for affiliation in affiliations:
        if "ror" in affiliation.lower():
            found, found_inst = _find_institution_in_temp("", affiliation)
        elif "ico" in affiliation.lower():
            ico = affiliation.split(": ")[-1]
            found, found_inst = _find_institution_in_temp("", None, ico)
        else:
            found, found_inst = _find_institution_in_temp(affiliation)

        if not found:
            raise ValueError(
                f"Affiliation: '{affiliation}' not found in the temporary institution vocabulary."
            )

        vocabulary_affiliations.append(found_inst)

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


def _find_institution(
    name: str, ror: Optional[str] = None, ico: Optional[str] = None
) -> Tuple[bool, Optional[str]]:
    """
    Check whether the given name and ror are present in the institutions vocabulary.
    """
    from invenio_access.permissions import system_identity
    from invenio_vocabularies.proxies import current_service

    escaped_name = lucene_escape(name)

    candidates = [
        "props.acronym",
        *[f"title.{lang}" for lang in LANGUAGES_IN_INSTITUTIONS],
        *[f"nonpreferredLabels.{lang}" for lang in LANGUAGES_IN_INSTITUTIONS],
    ]
    query_parts = [f'{candidate}:"{escaped_name}"' for candidate in candidates]
    query = " OR ".join(query_parts)

    if ror:
        query += f' OR relatedURI.ROR:"{ror}"'

    if ico:
        ico = ico.split(":")[-1].strip()
        query += f' OR props.ICO:"{ico}"'

    try:
        resp = current_service.search(
            system_identity, type="institutions", params={"q": query}
        )

        result = list(resp)[0]
        if "cs" in result["title"]:
            title = result["title"]["cs"]
        else:
            title = list(result["title"].values())[0]
        return True, title
    except IndexError:
        return False, None
    except Exception as e:
        log.error(
            f"Failed to search in institutions vocabulary with {name=} and {ror=}: {e}"
        )
        return False, None


def _find_institution_in_temp(
    name: str, ror: Optional[str] = None, ico: Optional[str] = None
) -> Tuple[bool, Optional[Dict[str, str]]]:
    """
    Check whether the given name and ror are present in the temporary institutions vocabulary.
    """
    found, found_inst = False, None
    for inst in TEMP_INSTITUTIONS:
        ico_is_matched = (
            ico
            and "props" in inst
            and "ICO" in inst["props"]
            and inst["props"]["ICO"] == ico
        )
        ror_is_matched = (
            ror
            and "relatedURI" in inst
            and "ROR" in inst["relatedURI"]
            and inst["relatedURI"]["ROR"] == ror
        )
        acronym_is_matched = (
            "props" in inst
            and "acronym" in inst["props"]
            and inst["props"]["acronym"] == name
        )
        language_title_is_matched = any(
            [
                name == inst["title"][lang]
                for lang in LANGUAGES_IN_INSTITUTIONS
                if lang in inst["title"]
            ]
        )

        if (
            ico_is_matched
            or ror_is_matched
            or acronym_is_matched
            or language_title_is_matched
        ):
            found, found_inst = True, inst

    if not found:
        return False, None

    if "cs" in found_inst["title"]:
        title = found_inst["title"]["cs"]
    else:
        title = list(found_inst["title"].values())[0]
    return True, {"id": inst["id"], "name": title}


def _find_creatibutor(identifiers: List[str]) -> Tuple[bool, Optional[Dict]]:
    """
    Check whether the given name and identifiers list are present in the RDM names vocabulary.
    """
    if not identifiers:
        return False, None

    from invenio_access.permissions import system_identity
    from invenio_vocabularies.proxies import current_service

    identifier_queries = []
    for idf in identifiers:
        identifier_queries.append(
            f"identifiers.scheme:{lucene_escape(idf['scheme'])} AND "
            + f"identifiers.identifier:{lucene_escape(idf['identifier'])}"
        )

    query = " OR ".join(f"({q})" for q in identifier_queries)

    try:
        resp = current_service.search(
            system_identity, type="names", params={"q": query}
        )
        results = list(resp)
        return True, results[0]
    except IndexError:
        return False, None
    except Exception as e:
        log.error(f"Failed to search in names vocabulary with {identifiers=}: {e}")
        return False, None


LANGUAGES_IN_INSTITUTIONS = [
    "cs",
    "en",
    "hu",
    "de",
    "fr",
    "la",
    "tr",
    "sk",
    "zh",
    "pl",
    "cy",
]
LUCENE_ESCAPE_CHARS = {
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
