import itertools
import Levenshtein
import logging
import sqlalchemy

from invenio_cache.proxies import current_cache

# will increase this in production
DEFAULT_VOCABULARY_CACHE_TTL = 3600

log = logging.getLogger("oaipmh.harvester")

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