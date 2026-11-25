"""
Industrial Two-Stage Deduplication Engine
========================================
Stage 1: SHA-256 Multi-Factor Fingerprint Hash Generation & Exact Match Check.
Stage 2: Multi-Attribute Fuzzy Similarity Engine (Company, Title, Skills, Location, Deadline).

Threshold Decision Matrix:
- Score > 0.97       -> Automatic Merge
- Score 0.90 - 0.97  -> Review Queue (Conservative Merge with review flag)
- Score < 0.90       -> Unique Opportunity

Explainable Decisions:
Every merge decision generates an explicit human-readable breakdown stored in
``score_vector["merge_explanation"]``.
"""

import hashlib
import re
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional, Set
from backend.pipeline.models import OpportunityNormalized


class OpportunityDeduplicator:
    """Industrial Two-Stage Deduplication Engine with Merge Explainability."""

    SENIOR_LEVELS = frozenset({"senior", "sr", "lead", "principal", "staff", "head", "director", "manager", "vp"})
    ENTRY_LEVELS = frozenset({"junior", "jr", "intern", "trainee", "fresher", "apprentice"})
    ATS_DOMAINS = ("greenhouse.io", "lever.co", "workday", "ashbyhq.com", "bamboohr.com", "smartrecruiters.com")

    TITLE_SYNONYMS = {
        "sde": "software development engineer",
        "swe": "software engineer",
        "dev": "developer",
        "qa": "quality assurance",
        "sdet": "software development engineer test",
        "internship": "intern",
    }

    @classmethod
    def generate_fingerprint(cls, opp: OpportunityNormalized) -> str:
        """Generate SHA-256 identity hash from core attributes.

        Identity answers "is this the same job?", not "is this the same
        version?". Deadline is therefore excluded: a re-crawl that extends the
        application deadline must not create a second card. A coarse posting
        month is included so a genuinely re-posted role in a later month is
        not permanently suppressed by the dedup skip path.
        """
        norm_company = re.sub(r'[^a-z0-9]', '', (opp.company or '').lower()).strip()
        norm_title = re.sub(r'[^a-z0-9]', '', (opp.title or '').lower()).strip()
        if not norm_company or not norm_title:
            return ""
        norm_loc = "remote" if opp.is_remote else re.sub(r'[^a-z0-9]', '', (opp.location or '').lower()).strip()
        posted_month = (
            opp.posted_at.strftime('%Y-%m') if opp.posted_at else ''
        )
        emp_type = (opp.opportunity_type or '').lower().strip()

        composite = f"{norm_company}|{norm_title}|{norm_loc}|{emp_type}|{posted_month}"
        return hashlib.sha256(composite.encode('utf-8')).hexdigest()

    @classmethod
    def normalize_title_words(cls, title: str) -> Set[str]:
        """Tokenize title, resolving common industry acronyms & synonyms."""
        raw_tokens = re.findall(r'\w+', (title or '').lower())
        tokens: Set[str] = set()
        for tok in raw_tokens:
            resolved = cls.TITLE_SYNONYMS.get(tok, tok)
            tokens.update(resolved.split())
        return tokens

    @classmethod
    def calculate_similarity_breakdown(cls, opp1: OpportunityNormalized, opp2: OpportunityNormalized) -> Dict[str, Any]:
        """Calculate multi-attribute similarity breakdown (Company, Title, Skills, Location, Deadline)."""
        t1_words = cls.normalize_title_words(opp1.title or '')
        t2_words = cls.normalize_title_words(opp2.title or '')

        has_senior_1 = bool(t1_words & cls.SENIOR_LEVELS)
        has_senior_2 = bool(t2_words & cls.SENIOR_LEVELS)
        has_entry_1 = bool(t1_words & cls.ENTRY_LEVELS)
        has_entry_2 = bool(t2_words & cls.ENTRY_LEVELS)

        reasons: List[str] = []

        # Seniority Guardrail
        if (has_senior_1 and has_entry_2) or (has_entry_1 and has_senior_2):
            reasons.append("Seniority mismatch (Senior vs Entry/Intern)")
            return {
                "total_score": 0.40,
                "company_score": 0.0,
                "title_score": 0.0,
                "skills_score": 0.0,
                "location_score": 0.0,
                "deadline_score": 0.0,
                "decision": "unique",
                "reasons": reasons,
            }

        # Location Guardrail
        loc1 = (opp1.location or '').lower().strip()
        loc2 = (opp2.location or '').lower().strip()
        if not opp1.is_remote and not opp2.is_remote:
            if loc1 and loc2 and loc1 != loc2 and loc1 not in loc2 and loc2 not in loc1:
                reasons.append("Physical city mismatch")
                return {
                    "total_score": 0.60,
                    "company_score": 0.0,
                    "title_score": 0.0,
                    "skills_score": 0.0,
                    "location_score": 0.0,
                    "deadline_score": 0.0,
                    "decision": "unique",
                    "reasons": reasons,
                }

        # 1. Company Score (Weight: 0.30)
        comp1 = (opp1.company_id or re.sub(r'[^a-z0-9]', '', (opp1.company or '').lower())).strip()
        comp2 = (opp2.company_id or re.sub(r'[^a-z0-9]', '', (opp2.company or '').lower())).strip()
        if comp1 and comp2 and comp1 == comp2:
            comp_score = 1.0
            reasons.append(f"Exact company match: '{opp1.company}'")
        elif comp1 and comp2 and (comp1 in comp2 or comp2 in comp1) and len(min(comp1, comp2, key=len)) >= 4:
            comp_score = 0.80
            reasons.append(f"Partial company match: '{opp1.company}' vs '{opp2.company}'")
        else:
            comp_score = 0.0

        # 2. Title Score (Weight: 0.30)
        if not t1_words or not t2_words:
            title_score = 0.0
        else:
            intersection = len(t1_words & t2_words)
            union = len(t1_words | t2_words)
            title_score = intersection / union
            if title_score < 0.9 and (t1_words.issubset(t2_words) or t2_words.issubset(t1_words)):
                title_score = max(title_score, 0.85)
            reasons.append(f"Title similarity: {int(title_score * 100)}%")

        # 3. Skills Score (Weight: 0.15)
        skills1 = set(s.lower() for s in (opp1.skills_required or []))
        skills2 = set(s.lower() for s in (opp2.skills_required or []))
        if skills1 and skills2:
            skills_score = len(skills1 & skills2) / len(skills1 | skills2)
            reasons.append(f"Skills overlap: {int(skills_score * 100)}%")
        else:
            skills_score = title_score

        # 4. Location Score (Weight: 0.15)
        if opp1.is_remote and opp2.is_remote:
            loc_score = 1.0
            reasons.append("Location match: Both Remote")
        elif loc1 and loc2 and (loc1 == loc2 or loc1 in loc2 or loc2 in loc1):
            loc_score = 1.0
            reasons.append(f"Location match: '{opp1.location}'")
        else:
            loc_score = 0.0

        # 5. Deadline Score (Weight: 0.10)
        if opp1.deadline and opp2.deadline:
            days_diff = abs((opp1.deadline - opp2.deadline).days)
            if days_diff == 0:
                deadline_score = 1.0
                reasons.append("Exact deadline match")
            elif days_diff <= 3:
                deadline_score = 0.80
                reasons.append(f"Close deadline (diff: {days_diff} days)")
            else:
                deadline_score = 0.0
        else:
            deadline_score = 0.50

        total_score = round(
            (comp_score * 0.30) + (title_score * 0.30) + (skills_score * 0.15) + (loc_score * 0.15) + (deadline_score * 0.10),
            4
        )

        decision = "unique"
        if total_score > 0.97:
            decision = "automatic_merge"
        elif total_score >= 0.90:
            decision = "review_queue"

        return {
            "total_score": total_score,
            "company_score": comp_score,
            "title_score": title_score,
            "skills_score": skills_score,
            "location_score": loc_score,
            "deadline_score": deadline_score,
            "decision": decision,
            "reasons": reasons,
        }

    @classmethod
    def calculate_similarity(cls, opp1: OpportunityNormalized, opp2: OpportunityNormalized) -> float:
        """Calculate overall multi-attribute similarity score (0.0 to 1.0)."""
        breakdown = cls.calculate_similarity_breakdown(opp1, opp2)
        return breakdown["total_score"]

    @staticmethod
    def _company_key(opp: OpportunityNormalized) -> str:
        """Normalized company identity used for the Stage-2 candidate block."""
        return (
            opp.company_id
            or re.sub(r'[^a-z0-9]', '', (opp.company or '').lower())
        ).strip()

    @classmethod
    def _companies_can_match(cls, key1: str, key2: str) -> bool:
        """Whether the company component can reach a mergeable score.

        Company carries weight 0.30 and all other components can contribute
        at most 0.70, so pairs whose companies are neither equal nor
        (>=4-char) substrings cannot exceed 0.70 — below the 0.90 review
        threshold. They never need the expensive similarity breakdown.
        """
        if not key1 or not key2:
            return False
        if key1 == key2:
            return True
        shorter, longer = (key1, key2) if len(key1) <= len(key2) else (key2, key1)
        return len(shorter) >= 4 and shorter in longer

    def deduplicate_batch(
        self,
        candidates: List[OpportunityNormalized]
    ) -> Tuple[List[OpportunityNormalized], int]:
        """Deduplicate a batch of incoming normalized opportunities using two-stage threshold rules.

        Returns:
            Tuple of (Deduplicated List, Count of Duplicates Removed)
        """
        seen_fingerprints: Dict[str, OpportunityNormalized] = {}
        unique_opportunities: List[OpportunityNormalized] = []
        # Stage-2 blocking index: company key -> positions in
        # ``unique_opportunities``. The full pairwise scan was O(n^2) with an
        # expensive title/skill tokenization per pair; a 7,436-item live
        # Unstop run sat in this stage for 15+ minutes. Company compatibility
        # is a necessary condition for any merge (see _companies_can_match),
        # so comparing only compatible blocks is exactly equivalent.
        company_positions: Dict[str, Set[int]] = {}
        duplicates_removed = 0

        for candidate in candidates:
            fp = self.generate_fingerprint(candidate)
            candidate = candidate.copy_with(fingerprint_hash=fp)

            # Stage 1: SHA-256 Exact Fingerprint Match
            if fp in seen_fingerprints:
                existing = seen_fingerprints[fp]
                old_company = self._company_key(existing)
                explanation = {
                    "stage": 1,
                    "type": "exact_fingerprint_match",
                    "fingerprint": fp,
                    "reasons": ["SHA-256 exact fingerprint match"],
                }
                merged = self._merge_opportunities(existing, candidate, explanation)
                index = unique_opportunities.index(existing)
                unique_opportunities[index] = merged
                seen_fingerprints[fp] = merged
                new_company = self._company_key(merged)
                if new_company and new_company != old_company:
                    company_positions.setdefault(new_company, set()).add(index)
                duplicates_removed += 1
                continue

            # Stage 2: Multi-Attribute Fuzzy Similarity Engine.
            # Only company-compatible uniques can cross the 0.90 threshold.
            candidate_company = self._company_key(candidate)
            candidate_positions: Set[int] = set(
                company_positions.get(candidate_company, ())
            )
            if candidate_company:
                for company_key, positions in company_positions.items():
                    if company_key == candidate_company:
                        continue
                    if self._companies_can_match(candidate_company, company_key):
                        candidate_positions.update(positions)

            is_merged = False
            for position in sorted(candidate_positions):
                existing = unique_opportunities[position]
                breakdown = self.calculate_similarity_breakdown(existing, candidate)
                score = breakdown["total_score"]

                if score > 0.97:
                    # Threshold > 0.97 -> Automatic Merge
                    explanation = {
                        "stage": 2,
                        "type": "automatic_merge",
                        "similarity_score": score,
                        "breakdown": breakdown,
                    }
                    old_company = self._company_key(existing)
                    merged = self._merge_opportunities(existing, candidate, explanation)
                    unique_opportunities[position] = merged
                    seen_fingerprints[existing.fingerprint_hash] = merged
                    new_company = self._company_key(merged)
                    if new_company and new_company != old_company:
                        company_positions.setdefault(new_company, set()).add(position)
                    duplicates_removed += 1
                    is_merged = True
                    break
                elif score >= 0.90:
                    # Threshold 0.90 - 0.97 -> Review Queue (Conservative Merge)
                    explanation = {
                        "stage": 2,
                        "type": "review_queue",
                        "similarity_score": score,
                        "breakdown": breakdown,
                    }
                    old_company = self._company_key(existing)
                    merged = self._merge_opportunities(existing, candidate, explanation)
                    merged = merged.copy_with(
                        score_vector={**merged.score_vector, 'needs_review': 1.0, 'merge_explanation': explanation}
                    )
                    unique_opportunities[position] = merged
                    seen_fingerprints[existing.fingerprint_hash] = merged
                    new_company = self._company_key(merged)
                    if new_company and new_company != old_company:
                        company_positions.setdefault(new_company, set()).add(position)
                    duplicates_removed += 1
                    is_merged = True
                    break

            if not is_merged:
                if candidate_company:
                    company_positions.setdefault(candidate_company, set()).add(
                        len(unique_opportunities)
                    )
                seen_fingerprints[fp] = candidate
                unique_opportunities.append(candidate)

        return unique_opportunities, duplicates_removed

    @classmethod
    def _merge_opportunities(
        cls,
        target: OpportunityNormalized,
        source: OpportunityNormalized,
        explanation: Dict[str, Any]
    ) -> OpportunityNormalized:
        """Merge higher quality fields from source into target with explicit merge explanation."""
        stipend_amount = target.stipend_amount
        salary_range = target.salary_range
        if (source.stipend_amount or 0) > (target.stipend_amount or 0):
            stipend_amount = source.stipend_amount
            salary_range = source.salary_range

        deadline = target.deadline
        if source.deadline and (target.deadline is None or source.deadline < target.deadline):
            deadline = source.deadline

        combined_skills = list(dict.fromkeys((target.skills_required or []) + (source.skills_required or [])))
        combined_degrees = list(dict.fromkeys((target.degrees_required or []) + (source.degrees_required or [])))

        desc = target.description
        if len(source.description or '') > len(target.description or ''):
            desc = source.description

        apply_url = target.apply_url
        if any(ats in (source.apply_url or '').lower() for ats in cls.ATS_DOMAINS):
            apply_url = source.apply_url

        company_id = target.company_id or source.company_id

        prev_sources = (target.raw_payload or {}).get("_merged_sources", [target.source_slug])
        if source.source_slug not in prev_sources:
            prev_sources.append(source.source_slug)

        merged_payload = {**(target.raw_payload or {}), "_merged_sources": prev_sources}
        score_vec = {**(target.score_vector or {}), "merge_explanation": explanation}

        return target.copy_with(
            company_id=company_id,
            description=desc,
            apply_url=apply_url,
            stipend_amount=stipend_amount,
            salary_range=salary_range,
            deadline=deadline,
            skills_required=combined_skills,
            degrees_required=combined_degrees,
            quality_score=max(target.quality_score, source.quality_score),
            trust_score=max(target.trust_score, source.trust_score),
            raw_payload=merged_payload,
            score_vector=score_vec,
        )


__all__ = ["OpportunityDeduplicator"]
