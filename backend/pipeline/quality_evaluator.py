"""
Explainable Quality Engine
=========================
Computes Explainable Quality Score, Trust Score, Source Reputation Weight,
Dynamic Freshness Decay Score, and Component Vector Score Composition.
"""

import re
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, List
from backend.pipeline.models import OpportunityNormalized
from backend.pipeline.student_signal_extractor import extract_student_signals


class QualityEvaluator:
    """Production Multi-Factor Scorer & Explainable Quality Engine."""

    ATS_DOMAINS = ("greenhouse.io", "lever.co", "workday", "ashbyhq.com", "bamboohr.com", "smartrecruiters.com")
    GOV_DOMAINS = (".gov.in", ".nic.in", ".ac.in", "pmrf.in", "upsc.gov.in", "ssc.nic.in")
    HACKATHON_DOMAINS = ("unstop.com", "devfolio.co", "hackerearth.com", "kaggle.com")
    SCHOLARSHIP_DOMAINS = ("buddy4study.com", "scholarships.gov.in", "tatatrusts.org", "daad.de")
    FREELANCE_DOMAINS = ("upwork.com", "fiverr.com", "freelancer.com", "toptal.com")

    DEFAULT_SOURCE_REPUTATIONS: Dict[str, int] = {
        "greenhouse": 95,
        "lever": 92,
        "ashby": 92,
        "devfolio": 90,
        "unstop": 85,
        "naukri": 75,
        "internshala": 75,
        "wellfound": 85,
        "freelance_gigs": 85,
        "scholarships": 85,
        "govt_rss": 90,
        "govt_research": 90,
    }

    SCAM_KEYWORDS = (
        "registration fee", "application fee", "pay to apply", "processing fee",
        "interview fee", "telegram contact", "whatsapp for selection",
        "security deposit", "guaranteed selection fee", "payment required",
        "pay upfront", "deposit required"
    )

    DISCLAIMER_PATTERNS = (
        r"\b(?:no|without\s+any)\s+(?:registration|application|interview|processing|selection|exam|examination|test|hiring|hidden)?\s*(?:fee|fees|charges?|deposit|payment)(?:\s+(?:or|and)\s+(?:application\s+charges|charges?|fees?|deposit|payment))?\b",
        r"\b(?:we\s+|[a-zA-Z0-9_\-]+\s+)?(?:never|do\s+not|does\s+not|don'?t)\s+(?:ask|asks|charge|charges|demand|demands|request|requests|require|requires|solicit|solicits|collect|collects)\s+(?:for\s+)?(?:any\s+)?(?:payment|fee|fees|money|charges?|deposit|amount)(?:\s+(?:or|and)\s+(?:security\s+deposit|deposit|fee|fees|charges?|payment))?\b",
        r"\bfree\s+(?:application|registration|entry|recruitment|of\s+cost|of\s+charge)\b",
        r"\b(?:application|registration|hiring|recruitment)\s+is\s+free\b",
        r"\bbeware\s+of\s+(?:fraudulent|scam|scams|fake|unauthorized|fraud)?\s*(?:charges|fees|payments|agents|recruiters|calls|job\s+offers)?\b",
        r"\b(?:candidates?\s+(?:are\s+)?(?:advised|warned|requested)\s+)?(?:do\s+not|never|not\s+to)\s+pay\s+(?:any\s+)?(?:fee|fees|money|amount|deposit|charges?)(?:\s+(?:or|and)\s+(?:deposit|fee|fees|money|amount|charges?))?\b",
        r"\bno\s+(?:fee|fees|charges?|payment)\s+(?:is|are)?\s*(?:charged|required|payable|collected|applicable|levied)\b",
    )

    CONTACT_ANOMALY_PATTERNS = {
        "telegram": (
            r"\bt\.me/[a-zA-Z0-9_+]+",
            r"\btelegram\.me/[a-zA-Z0-9_+]+",
            r"@telegram\b",
            r"\b(?:contact|msg|message|reach\s+out|apply)\s+(?:on|via|through)\s+telegram\b",
            r"\btelegram\s+(?:contact|channel|group|handle|id)\b",
        ),
        "whatsapp": (
            r"\bwa\.me/[0-9]+",
            r"\bapi\.whatsapp\.com/send",
            r"\bchat\.whatsapp\.com/[a-zA-Z0-9]+",
            r"\bwhatsapp\s+(?:for\s+)?(?:selection|interview|cv|resume|hiring|contact)\b",
            r"\b(?:send|forward|share)\s+(?:your\s+)?(?:cv|resume)\s+(?:on|to|via)\s+whatsapp\b",
            r"\bcontact\s+(?:hr\s+)?(?:on|via)\s+whatsapp\b",
        ),
        "upi": (
            r"\b[a-zA-Z0-9.\-_]{2,64}@(upi|okaxis|okhdfcbank|okicici|oksbi|ybl|paytm|ibl|axl)\b",
            r"\b(?:gpay|phonepe|paytm)\s*(?:no|number|id)?\s*[:=]\s*[0-9]{10}\b",
        ),
    }

    _COMPILED_DISCLAIMERS = [re.compile(p, re.IGNORECASE) for p in DISCLAIMER_PATTERNS]
    _COMPILED_CONTACTS = {
        k: [re.compile(p, re.IGNORECASE) for p in v]
        for k, v in CONTACT_ANOMALY_PATTERNS.items()
    }
    _COMPILED_SCAM_KEYWORDS = [
        re.compile(r"\b" + re.escape(kw) + r"\b", re.IGNORECASE)
        for kw in SCAM_KEYWORDS
    ]

    @classmethod
    def _mask_disclaimers(cls, text: str) -> str:
        """Mask legitimate anti-fraud disclaimers with placeholder to avoid false positives."""
        if not text:
            return ""
        masked = text
        for pat in cls._COMPILED_DISCLAIMERS:
            masked = pat.sub(" __LEGITIMATE_DISCLAIMER__ ", masked)
        return masked

    @classmethod
    def _detect_contact_anomalies(cls, text: str, apply_url: str) -> Tuple[float, List[str]]:
        """Detect off-platform contact and payment anomalies."""
        combined = f"{text or ''} {apply_url or ''}".lower()
        score = 0.0
        anomalies: List[str] = []

        for pat in cls._COMPILED_CONTACTS["telegram"]:
            if pat.search(combined):
                score += 0.30
                anomalies.append("Telegram recruitment channel")
                break

        for pat in cls._COMPILED_CONTACTS["whatsapp"]:
            if pat.search(combined):
                score += 0.30
                anomalies.append("WhatsApp selection channel")
                break

        for pat in cls._COMPILED_CONTACTS["upi"]:
            if pat.search(combined):
                score += 0.50
                anomalies.append("Personal UPI/direct payment handle")
                break

        return min(0.60, score), anomalies

    def _compute_domain_risk(self, opp: OpportunityNormalized) -> float:
        """Calculate source domain reputation baseline risk (0.0 to 0.20)."""
        url_lower = (opp.apply_url or '').lower()
        source_slug = (opp.source_slug or '').lower().strip()
        default_rep = self.DEFAULT_SOURCE_REPUTATIONS.get(source_slug, 50)
        source_rep = min(100, max(0, opp.source_reputation if opp.source_reputation is not None else default_rep))

        if (
            any(ats in url_lower for ats in self.ATS_DOMAINS)
            or any(gov in url_lower for gov in self.GOV_DOMAINS)
            or any(h in url_lower for h in self.HACKATHON_DOMAINS)
            or any(s in url_lower for s in self.SCHOLARSHIP_DOMAINS)
            or any(f in url_lower for f in self.FREELANCE_DOMAINS)
        ):
            return 0.0

        if source_rep >= 80:
            return 0.0

        return round(((80 - source_rep) / 80.0) * 0.20, 4)

    def evaluate(self, opp: OpportunityNormalized) -> OpportunityNormalized:
        """Evaluate and compute all scores for a normalized opportunity with full explainability."""
        # 1. Quality Score Breakdown (0-100) & Explanations
        quality_breakdown: Dict[str, Any] = {}
        explanations: Dict[str, str] = {}

        # Component A: Description Completeness (Max: 25 pts)
        desc_len = len(opp.description or '')
        if desc_len >= 1000:
            quality_breakdown['description_completeness'] = 25
            explanations['description_completeness'] = "Comprehensive description (>= 1000 characters)"
        elif desc_len >= 500:
            quality_breakdown['description_completeness'] = 20
            explanations['description_completeness'] = "Detailed description (>= 500 characters)"
        elif desc_len >= 200:
            quality_breakdown['description_completeness'] = 15
            explanations['description_completeness'] = "Moderate description (>= 200 characters)"
        elif desc_len >= 50:
            quality_breakdown['description_completeness'] = 10
            explanations['description_completeness'] = "Brief description (>= 50 characters)"
        else:
            quality_breakdown['description_completeness'] = 5
            explanations['description_completeness'] = "Minimal description (< 50 characters)"

        # Domain Context Extraction
        category = (opp.category or '').lower().strip()
        opp_type = (opp.opportunity_type or '').lower().strip()
        raw = opp.raw_payload if isinstance(opp.raw_payload, dict) else {}
        subcat = str(raw.get("subcategory") or '').lower().strip()
        title_lower = (opp.title or '').lower()
        desc_lower = (opp.description or '').lower()

        # Component B: Compensation Clarity (Max: 20 pts)
        if opp.stipend_amount and opp.stipend_amount > 0:
            quality_breakdown['compensation_clarity'] = 20
            explanations['compensation_clarity'] = f"Explicit monetary stipend specified: ₹{int(opp.stipend_amount):,}"
        elif opp_type == 'competition' or subcat == 'hackathon' or 'hackathon' in title_lower:
            prize_val = raw.get('prize') or raw.get('award') or raw.get('prize_pool')
            if prize_val:
                quality_breakdown['compensation_clarity'] = 20
                explanations['compensation_clarity'] = f"Explicit competition reward/prize pool specified: {prize_val}"
            elif raw.get('is_free') or 'free' in title_lower or 'cash prize' in desc_lower or 'prizes' in desc_lower:
                quality_breakdown['compensation_clarity'] = 20
                explanations['compensation_clarity'] = "Free entry competition with awards disclosed"
            else:
                quality_breakdown['compensation_clarity'] = 15
                explanations['compensation_clarity'] = "Standard competition rewards & terms disclosed"
        elif opp_type == 'scholarship' or subcat == 'scholarship' or 'scholarship' in title_lower:
            award_val = raw.get('award_amount') or raw.get('amount') or raw.get('grant')
            if award_val:
                quality_breakdown['compensation_clarity'] = 20
                explanations['compensation_clarity'] = f"Explicit scholarship aid/award amount specified: {award_val}"
            elif 'tuition' in desc_lower or 'fully funded' in desc_lower or 'financial aid' in desc_lower or 'fellowship' in desc_lower:
                quality_breakdown['compensation_clarity'] = 20
                explanations['compensation_clarity'] = "Educational grant / tuition coverage explicitly detailed"
            else:
                quality_breakdown['compensation_clarity'] = 15
                explanations['compensation_clarity'] = "Academic scholarship eligibility & benefits specified"
        elif category == 'government':
            sal_range = (opp.salary_range or '').lower()
            if any(k in sal_range for k in ("level", "scale", "matrix", "cpc", "pb-")) or any(k in desc_lower for k in ("pay matrix", "level 10", "level 11", "level 7", "level 8", "7th cpc", "pay scale")):
                quality_breakdown['compensation_clarity'] = 20
                explanations['compensation_clarity'] = f"Official government pay scale/matrix specified: '{opp.salary_range or 'Statutory Pay Matrix'}'"
            elif opp.salary_range and 'unpaid' not in sal_range:
                quality_breakdown['compensation_clarity'] = 20
                explanations['compensation_clarity'] = f"Government compensation scale specified: '{opp.salary_range}'"
            else:
                quality_breakdown['compensation_clarity'] = 20
                explanations['compensation_clarity'] = "Statutory government gazetted service rules apply"
        elif category == 'freelance' or opp_type == 'freelance':
            budget_val = raw.get('budget') or raw.get('hourly_rate') or opp.salary_range
            if budget_val and 'unpaid' not in str(budget_val).lower():
                quality_breakdown['compensation_clarity'] = 20
                explanations['compensation_clarity'] = f"Explicit freelance project budget/rate specified: '{budget_val}'"
            else:
                quality_breakdown['compensation_clarity'] = 15
                explanations['compensation_clarity'] = "Freelance engagement terms specified"
        elif opp.salary_range and 'unpaid' not in opp.salary_range.lower():
            quality_breakdown['compensation_clarity'] = 15
            explanations['compensation_clarity'] = f"Salary range specified: '{opp.salary_range}'"
        elif opp.salary_range and 'unpaid' in opp.salary_range.lower():
            quality_breakdown['compensation_clarity'] = 8
            explanations['compensation_clarity'] = "Unpaid opportunity explicitly disclosed"
        else:
            quality_breakdown['compensation_clarity'] = 5
            explanations['compensation_clarity'] = "Compensation not listed"

        # Component C: Source Reputation (Max: 20 pts)
        source_slug = (opp.source_slug or '').lower().strip()
        default_rep = self.DEFAULT_SOURCE_REPUTATIONS.get(source_slug, 50)
        source_rep = min(100, max(0, opp.source_reputation if opp.source_reputation is not None else default_rep))
        rep_pts = int((source_rep / 100.0) * 20)
        quality_breakdown['source_reputation'] = rep_pts
        explanations['source_reputation'] = f"Source reputation weight: {source_rep}/100"

        # Component D: Structured Metadata (Skills, Degrees, Branch) (Max: 20 pts)
        skill_cnt = len(opp.skills_required or [])
        skill_pts = min(10, skill_cnt * 2)
        degree_pts = 5 if (opp.degrees_required and len(opp.degrees_required) > 0) else 0
        branch_pts = 5 if opp.branch else 0

        # Domain-aware metadata adjustment:
        if opp_type in ('competition', 'freelance') or category in ('freelance',) or subcat in ('hackathon',):
            # For Freelance & Hackathons: degrees and college branch are not standard requirements;
            # evaluate skill depth and experience/eligibility level.
            if skill_cnt >= 3:
                skill_pts = min(15, skill_cnt * 3)
            exp_pts = 5 if opp.experience_level and opp.experience_level != 'any' else (5 if skill_cnt >= 4 else 0)
            metadata_pts = min(20, skill_pts + exp_pts + degree_pts)
            explanations['structured_metadata'] = (
                f"Domain-adapted metadata: {skill_cnt} skills ({skill_pts}pts), "
                f"experience/eligibility ({exp_pts}pts)"
            )
        elif category == 'government':
            # Government notices focus on educational qualification/degree and syllabus skills
            gov_qual_pts = 10 if opp.degrees_required and len(opp.degrees_required) > 0 else 5
            gov_skill_pts = min(10, max(5, skill_cnt * 3))
            metadata_pts = min(20, gov_qual_pts + gov_skill_pts)
            explanations['structured_metadata'] = (
                f"Government statutory criteria: qualifications ({gov_qual_pts}pts), "
                f"syllabus/skills ({gov_skill_pts}pts)"
            )
        else:
            metadata_pts = min(20, skill_pts + degree_pts + branch_pts)
            explanations['structured_metadata'] = (
                f"Extracted {skill_cnt} skills ({skill_pts}pts), "
                f"degrees ({degree_pts}pts), branch ({branch_pts}pts)"
            )
        quality_breakdown['structured_metadata'] = metadata_pts

        # Component E: Apply URL Quality (Max: 15 pts)
        url_lower = (opp.apply_url or '').lower()
        if any(ats in url_lower for ats in self.ATS_DOMAINS):
            quality_breakdown['apply_url_quality'] = 15
            explanations['apply_url_quality'] = "Direct corporate Applicant Tracking System (ATS) link"
        elif any(gov in url_lower for gov in self.GOV_DOMAINS):
            quality_breakdown['apply_url_quality'] = 15
            explanations['apply_url_quality'] = "Official government or accredited institutional portal"
        elif any(h in url_lower for h in self.HACKATHON_DOMAINS):
            quality_breakdown['apply_url_quality'] = 15
            explanations['apply_url_quality'] = "Verified hackathon & competition hosting platform"
        elif any(s in url_lower for s in self.SCHOLARSHIP_DOMAINS):
            quality_breakdown['apply_url_quality'] = 15
            explanations['apply_url_quality'] = "Verified scholarship & financial aid portal"
        elif any(f in url_lower for f in self.FREELANCE_DOMAINS):
            quality_breakdown['apply_url_quality'] = 15
            explanations['apply_url_quality'] = "Verified freelance contract marketplace"
        elif url_lower.startswith('https://'):
            quality_breakdown['apply_url_quality'] = 10
            explanations['apply_url_quality'] = "Valid secure HTTPS application link"
        else:
            quality_breakdown['apply_url_quality'] = 5
            explanations['apply_url_quality'] = "Standard application link"

        # Student Signal Extraction & Experience Trap Gatekeeping
        student_signals = extract_student_signals(
            title=opp.title or "",
            description=opp.description or "",
            apply_url=opp.apply_url or "",
            experience_level=opp.experience_level or "entry",
            tags=opp.skills_required or [],
            required_degrees=opp.degrees_required or [],
            ppo_offered=bool(opp.ppo_offered),
            normalized_monthly_stipend=opp.normalized_monthly_stipend,
        )

        if student_signals.get("is_experience_trap"):
            quality_breakdown['experience_trap_penalty'] = -25
            explanations['experience_trap_penalty'] = (
                f"Fake-fresher experience trap: entry role requires "
                f"{student_signals.get('min_experience_months', 0)} months of experience (-25 pts)"
            )

        quality_total = sum(quality_breakdown.values())
        quality_score = min(100, max(0, quality_total))

        # 2. Fraud & Trust Score Computation
        raw_desc = opp.description or ''
        desc_lower = raw_desc.lower()
        masked_desc = self._mask_disclaimers(desc_lower)

        # Factor 1: Domain Reputation Risk
        domain_risk = self._compute_domain_risk(opp)

        # Factor 2: Un-negated Scam Keywords (strict word-boundary match)
        matched_scam_keywords = [
            pat.pattern for pat in self._COMPILED_SCAM_KEYWORDS if pat.search(masked_desc)
        ]
        scam_flag = len(matched_scam_keywords) > 0
        if not matched_scam_keywords:
            keyword_penalty = 0.0
        elif len(matched_scam_keywords) == 1:
            keyword_penalty = 0.65
        else:
            keyword_penalty = 0.85

        # Factor 3: Contact Channel Anomalies
        contact_anomaly_penalty, contact_anomalies = self._detect_contact_anomalies(masked_desc, opp.apply_url)

        # Tri-Factor Fraud Score Composition
        raw_fraud = domain_risk + keyword_penalty + contact_anomaly_penalty
        fraud_score = round(min(1.0, max(0.0, raw_fraud)), 4)

        # Inverse Trust Score Calibration
        trust_pts = opp.trust_score if (opp.trust_score and opp.trust_score > 0) else source_rep
        if scam_flag:
            penalty = max(60, int(fraud_score * 100))
            trust_score = max(0, trust_pts - penalty)
            trust_explanation = f"Flagged: Contains suspicious payment or fee keywords (fraud risk: {fraud_score:.2f})"
        elif fraud_score > 0.40:
            penalty = int(fraud_score * 100)
            trust_score = max(0, trust_pts - penalty)
            trust_explanation = f"Flagged: High fraud risk ({fraud_score:.2f}) with suspicious signals"
        elif fraud_score > 0.10:
            penalty = int(fraud_score * 50)
            trust_score = max(0, trust_pts - penalty)
            trust_explanation = f"Elevated fraud risk ({fraud_score:.2f}): minor trust reduction ({trust_score}/100)"
        else:
            trust_score = trust_pts
            trust_explanation = f"Trusted posting from verified source reputation ({trust_score}/100)"

        # 3. Dynamic Freshness Score Computation (100 = Today, 85 = 3 days, 70 = 7 days, 30 = 30 days)
        now = datetime.now(timezone.utc)
        posted = opp.posted_at
        if posted is not None and posted.tzinfo is None:
            posted = posted.replace(tzinfo=timezone.utc)

        if posted is None:
            posted = now

        age_days = max(0, (now - posted).days)

        if age_days == 0:
            freshness_score = 100
            freshness_explanation = "Posted today"
        elif age_days <= 3:
            freshness_score = 85
            freshness_explanation = f"Posted {age_days} days ago"
        elif age_days <= 7:
            freshness_score = 70
            freshness_explanation = f"Posted {age_days} days ago (within 1 week)"
        elif age_days <= 30:
            freshness_score = 30
            freshness_explanation = f"Posted {age_days} days ago (within 1 month)"
        else:
            freshness_score = 10
            freshness_explanation = f"Posted {age_days} days ago (older than 1 month)"

        # 4. Component Vector Score Persistence & Explainability
        fraud_explanation = "Low fraud risk"
        if fraud_score >= 0.80:
            fraud_explanation = "Critical: Definite fraud detected"
        elif fraud_score > 0.40:
            fraud_explanation = "Warning: Elevated fraud signals"

        score_vector: Dict[str, Any] = {
            'relevance': 0.85,
            'quality': round(quality_score / 100.0, 4),
            'trust': round(trust_score / 100.0, 4),
            'fraud': round(fraud_score, 4),
            'source_reputation': round(source_rep / 100.0, 4),
            'freshness': round(freshness_score / 100.0, 4),
            'urgency': 1.0 if opp.lifecycle_state == 'CLOSING_SOON' else 0.5,
            'penalty': 1.0 if opp.lifecycle_state == 'EXPIRED' else 0.0,
            'popularity': round((opp.popularity_score or 50) / 100.0, 4),
            'final_score': 0.0,
            'explanations': {
                'quality': f"Quality Score {quality_score}/100",
                'trust': trust_explanation,
                'fraud': f"Fraud Score {fraud_score:.2f} ({fraud_explanation})",
                'source_reputation': f"Source Reputation {source_rep}/100",
                'freshness': freshness_explanation,
                'popularity': f"Popularity Score {(opp.popularity_score or 50)}/100",
                'components': explanations,
            }
        }
        if contact_anomalies:
            score_vector['explanations']['contact_anomalies'] = ", ".join(contact_anomalies)

        # Weighted Final Vector Composition
        final_score = (
            (score_vector['relevance'] * 0.30) +
            (score_vector['quality'] * 0.25) +
            (score_vector['trust'] * 0.15) +
            (score_vector['freshness'] * 0.15) +
            (score_vector['urgency'] * 0.15) -
            (score_vector['penalty'] * 1.0)
        )
        score_vector['final_score'] = round(max(0.0, final_score), 4)

        # Complete quality breakdown artifact containing components & explanation strings
        full_quality_breakdown = {
            **quality_breakdown,
            "components": quality_breakdown,
            "explanations": explanations,
            "quality_score": quality_score,
            "trust_score": trust_score,
            "fraud_score": fraud_score,
            "source_reputation": source_rep,
            "freshness_score": freshness_score,
            "fraud_factors": {
                "domain_risk": domain_risk,
                "keyword_penalty": keyword_penalty,
                "contact_anomaly_penalty": contact_anomaly_penalty,
                "matched_keywords": matched_scam_keywords,
                "contact_anomalies": contact_anomalies,
            },
        }

        updated_metadata = {
            **(opp.metadata or {}),
            **student_signals,
        }

        return opp.copy_with(
            quality_score=quality_score,
            trust_score=trust_score,
            freshness_score=freshness_score,
            fraud_score=fraud_score,
            quality_breakdown=full_quality_breakdown,
            score_vector=score_vector,
            metadata=updated_metadata,
        )


__all__ = ["QualityEvaluator"]
