"""
inventory.py
============
Curated stat facts for scheduled tweets — cream of the crop only.
Every fact here must be: genuinely surprising, number-backed, debate-worthy.

Tweet assembled as:
    {hook}\n\n{body}\n\n📊 {url}\n{hashtags}

section values → which cinetrace page/section to screenshot:
  "directors"    → actor page "Directors Worked With" section
  "collaborators"→ actor page "By the Numbers" insight cards
  "blockbusters" → actor page "Blockbusters" section
  "overview"     → actor page "By the Numbers" insight cards (general stats)
  "filmography"  → actor page "By the Numbers" (decade/career-count facts)
  "compare"      → /compare/{actor}-vs-{compare_with} page (needs compare_with field)
"""

from __future__ import annotations

FACTS: dict[str, list[dict]] = {

    # ── Kamal Haasan ──────────────────────────────────────────────────────────

    "Kamal Haasan": [
        {
            "key":          "kh_vs_rajini",
            "section":      "compare",
            "compare_with": "rajinikanth",
            "hook":    "Kamal Haasan has 54 MORE films than Rajinikanth.",
            "body":    "246 vs 192.\nYet the debate about who is bigger never ends.\nThat is the magic of Rajini.",
            "hashtags": "#KamalHaasan #Rajinikanth #Kollywood",
        },
        {
            "key":     "kh_70s_insane",
            "section": "filmography",
            "hook":    "Kamal Haasan made 104 films in the 1970s alone.",
            "body":    "That single decade has more films than Allu Arjun's entire career.\n104 films. One decade. One man.",
            "hashtags": "#KamalHaasan #Kollywood #SouthCinema",
        },
        {
            "key":     "kh_65_years",
            "section": "overview",
            "hook":    "Kamal Haasan has been acting for 65 years.",
            "body":    "Started in 1960 as a child actor.\nStill headlining films in 2025.\nNo other South Indian actor has an active career this long.",
            "hashtags": "#KamalHaasan #Kollywood #SouthCinema",
        },
        {
            "key":     "kh_rajini_costars",
            "section": "collaborators",
            "hook":    "Kamal Haasan and Rajinikanth were co-stars in 36 films.",
            "body":    "They shared the screen 36 times before becoming parallel legends.\nThe greatest rivalry in Indian cinema started as a friendship.",
            "hashtags": "#KamalHaasan #Rajinikanth #Kollywood",
        },
        {
            "key":     "kh_kb_29",
            "section": "directors",
            "hook":    "One director worked with Kamal Haasan 29 times.",
            "body":    "K. Balachander directed Kamal 29 times — the longest actor-director partnership in Tamil film history.\nAlmost 1 in every 8 Kamal films.",
            "hashtags": "#KamalHaasan #Kollywood",
        },
        {
            "key":     "kh_659_collabs",
            "section": "collaborators",
            "hook":    "659 actors have worked with Kamal Haasan.",
            "body":    "The most connected actor in South Indian cinema — by a wide margin.\nMore than any other actor on this list.",
            "hashtags": "#KamalHaasan #Kollywood #SouthCinema",
        },
    ],

    # ── Rajinikanth ───────────────────────────────────────────────────────────

    "Rajinikanth": [
        {
            "key":     "rj_85_in_80s",
            "section": "filmography",
            "hook":    "Rajinikanth did 85 films in just the 1980s.",
            "body":    "That is nearly 9 films a year for a full decade.\nThe entire filmographies of Yash, Ram Charan, and Allu Arjun — combined — don't match that.",
            "hashtags": "#Rajinikanth #Superstar #Kollywood",
        },
        {
            "key":     "rj_kamal_36",
            "section": "collaborators",
            "hook":    "Rajinikanth and Kamal were co-stars in 35 films.",
            "body":    "Before they became icons, they were each other's screen partners.\n35 films together. Then two separate empires.",
            "hashtags": "#Rajinikanth #KamalHaasan #Kollywood",
        },
        {
            "key":     "rj_sp_24",
            "section": "directors",
            "hook":    "The same director made 24 films with Rajinikanth.",
            "body":    "S.P. Muthuraman directed him 24 times — that is 1 in every 8 Rajini films.\nMost fans have never heard this name.",
            "hashtags": "#Rajinikanth #Superstar #Kollywood",
        },
        {
            "key":     "rj_5_langs",
            "section": "overview",
            "hook":    "Rajinikanth acted in 5 languages across his career.",
            "body":    "Tamil, Telugu, Hindi, Kannada, English.\nThe original pan-Indian star — before that term even existed.",
            "hashtags": "#Rajinikanth #Superstar #SouthCinema",
        },
    ],

    # ── Mammootty ─────────────────────────────────────────────────────────────

    "Mammootty": [
        {
            "key":     "mm_combined",
            "section": "overview",
            "hook":    "Mammootty has more films than Rajinikanth and Kamal Haasan combined.",
            "body":    "Rajini: 192. Kamal: 246. Total: 438.\nMammootty: 444.\nThe most prolific major film career in all of South India.",
            "hashtags": "#Mammootty #Mollywood #SouthCinema",
        },
        {
            "key":     "mm_80s_206",
            "section": "filmography",
            "hook":    "Mammootty made 206 films in the 1980s alone.",
            "body":    "That is more than Ram Charan, Allu Arjun, Yash, and Jr. NTR's entire careers — combined.\nOne decade. 206 films.",
            "hashtags": "#Mammootty #Mollywood #SouthCinema",
        },
        {
            "key":     "mm_sukumari_104",
            "section": "collaborators",
            "hook":    "The same actress appeared in 104 of Mammootty's films.",
            "body":    "Sukumari starred alongside him in 104 films.\nThat single co-star count is more than Allu Arjun's entire filmography.",
            "hashtags": "#Mammootty #Mollywood",
        },
        {
            "key":     "mm_700",
            "section": "collaborators",
            "hook":    "700 actors have worked with Mammootty. 700.",
            "body":    "The widest collaboration network in South Indian cinema.\nIn Mollywood, everyone has a Mammootty connection.",
            "hashtags": "#Mammootty #Mammukka #Mollywood",
        },
        {
            "key":     "mm_two_dirs_33",
            "section": "directors",
            "hook":    "Two different directors worked with Mammootty exactly 33 times each.",
            "body":    "Joshiy: 33 films. I.V. Sasi: 33 films.\nPerfect symmetry — no other actor in South India has this.",
            "hashtags": "#Mammootty #Mollywood",
        },
    ],

    # ── Mohanlal ──────────────────────────────────────────────────────────────

    "Mohanlal": [
        {
            "key":     "ml_priyan_36",
            "section": "directors",
            "hook":    "One director made 36 films with Mohanlal. 36.",
            "body":    "Priyadarshan directed him 36 times — the highest actor-director count on this list.\nThat partnership alone spans three decades.",
            "hashtags": "#Mohanlal #Lalettan #Mollywood",
        },
        {
            "key":     "ml_jagathy_114",
            "section": "collaborators",
            "hook":    "The same co-star appeared in 114 of Mohanlal's films.",
            "body":    "Jagathy Sreekumar: 114 films with Mohanlal.\nThat number is more than Allu Arjun's entire career.",
            "hashtags": "#Mohanlal #Mollywood",
        },
        {
            "key":     "ml_mamm_60",
            "section": "collaborators",
            "hook":    "Mohanlal and Mammootty shared the screen in 60 films.",
            "body":    "South India's two greatest parallel careers — and they appeared together 60 times.\nThe Mollywood golden era was built on this.",
            "hashtags": "#Mohanlal #Mammootty #Mollywood",
        },
        {
            "key":     "ml_80s_166",
            "section": "filmography",
            "hook":    "Mohanlal made 166 films in just the 1980s.",
            "body":    "That is over 16 films a year for a decade.\nAnd somehow, each role was different.",
            "hashtags": "#Mohanlal #Lalettan #Mollywood",
        },
    ],

    # ── Vijay ─────────────────────────────────────────────────────────────────

    "Vijay": [
        {
            "key":     "vj_dad_13",
            "section": "directors",
            "hook":    "Vijay's own father directed him 13 times.",
            "body":    "S.A. Chandrasekhar — Vijay's dad — directed him more than any other director in his career.\nThe Thalapathy factory started at home.",
            "hashtags": "#Vijay #Thalapathy #Kollywood",
        },
        {
            "key":          "vj_ajith_343",
            "section":      "compare",
            "compare_with": "ajith-kumar",
            "hook":    "Vijay and Ajith have worked with the EXACT same number of actors. 343 each.",
            "body":    "Same era. Same industry. Same network depth.\nThe biggest rivalry in Tamil cinema is mathematically even.",
            "hashtags": "#Vijay #Ajith #Kollywood",
        },
        {
            "key":     "vj_90s_24",
            "section": "filmography",
            "hook":    "Vijay made 24 films in the 1990s — before anyone knew his name.",
            "body":    "His busiest decade came before his biggest fame.\n24 films in the 90s. Most fans have seen none of them.",
            "hashtags": "#Vijay #Thalapathy #Kollywood",
        },
        {
            "key":     "vj_vadivelu_14",
            "section": "collaborators",
            "hook":    "Vadivelu appeared in 14 Vijay films.",
            "body":    "14 films together — more than most leading actors work with a single co-star in a lifetime.\nAsk any 90s Tamil kid which scenes they remember.",
            "hashtags": "#Vijay #Vadivelu #Kollywood",
        },
    ],

    # ── Ajith Kumar ───────────────────────────────────────────────────────────

    "Ajith Kumar": [
        {
            "key":          "ak_vijay_343",
            "section":      "compare",
            "compare_with": "vijay",
            "hook":    "Ajith and Vijay have worked with exactly 343 actors each.",
            "body":    "Same count. Same era. Same industry.\nThe biggest rivalry in Tamil cinema is perfectly balanced — the data says draw.",
            "hashtags": "#Ajith #Thala #Vijay #Kollywood",
        },
        {
            "key":     "ak_90s_26",
            "section": "filmography",
            "hook":    "Ajith made 26 films in the 1990s — before superstardom.",
            "body":    "His most productive decade was also his least celebrated.\n26 films before anyone called him Thala.",
            "hashtags": "#Ajith #Thala #Kollywood",
        },
        {
            "key":     "ak_no_loyalty",
            "section": "directors",
            "hook":    "No single director has worked with Ajith more than 4 times.",
            "body":    "Siva, Saran, H. Vinoth — all maxed at 4 films.\nHe has been equally fair to everyone. No one owns his career.",
            "hashtags": "#Ajith #Thala #Kollywood",
        },
    ],

    # ── Jr. NTR ───────────────────────────────────────────────────────────────

    "Jr. NTR": [
        {
            "key":     "ntr_raj_7",
            "section": "directors",
            "hook":    "Rajamouli directed Jr. NTR 7 times before RRR.",
            "body":    "RRR was their 7th collaboration — not their first.\nBy then they had a decade of trust already built.",
            "hashtags": "#JrNTR #Tarak #RRR #Tollywood",
        },
        {
            "key":     "ntr_brahma_16",
            "section": "collaborators",
            "hook":    "Brahmanandam appears in 16 of Jr. NTR's 43 films.",
            "body":    "More than 1 in every 3 NTR films has the same comedian.\nThe most consistent actor-comedian duo in Tollywood.",
            "hashtags": "#JrNTR #Brahmanandam #Tollywood",
        },
    ],

    # ── Ram Charan ────────────────────────────────────────────────────────────

    "Ram Charan": [
        {
            "key":     "rc_only_20",
            "section": "overview",
            "hook":    "Ram Charan has only 20 films. He is a global superstar.",
            "body":    "The most selective filmography of any top South Indian star.\n20 films. RRR. Worldwide fame. Less is more.",
            "hashtags": "#RamCharan #RRR #Tollywood",
        },
        {
            "key":     "rc_brahma_50pct",
            "section": "collaborators",
            "hook":    "Brahmanandam appears in exactly half of Ram Charan's films.",
            "body":    "10 out of 20 films. 50% of his entire career has the same comedian.\nThat is not coincidence. That is a formula.",
            "hashtags": "#RamCharan #Brahmanandam #Tollywood",
        },
        {
            "key":     "rc_ntr_5",
            "section": "collaborators",
            "hook":    "Ram Charan's most frequent co-star is Jr. NTR — 5 films together.",
            "body":    "Their bond goes way deeper than RRR.\nWhen the data says 5 films, the friendship finally makes sense.",
            "hashtags": "#RamCharan #JrNTR #RRR #Tollywood",
        },
    ],

    # ── Allu Arjun ────────────────────────────────────────────────────────────

    "Allu Arjun": [
        {
            "key":     "aa_sukumar_6",
            "section": "directors",
            "hook":    "Allu Arjun worked with the same director 6 times.",
            "body":    "Sukumar directed Bunny 6 times — more than any other director in his career.\nEvery Pushpa record was built on this one partnership.",
            "hashtags": "#AlluArjun #Bunny #Pushpa #Tollywood",
        },
        {
            "key":     "aa_brahma_42pct",
            "section": "collaborators",
            "hook":    "Brahmanandam is in 42% of Allu Arjun's movies.",
            "body":    "14 out of 33 films.\nNearly half of Bunny's filmography has the same comedian in it.",
            "hashtags": "#AlluArjun #Brahmanandam #Tollywood",
        },
    ],

    # ── Prabhas ───────────────────────────────────────────────────────────────

    "Prabhas": [
        {
            "key":     "pb_raj_8",
            "section": "directors",
            "hook":    "SS Rajamouli directed Prabhas 8 times.",
            "body":    "No other actor has been directed this often by India's biggest filmmaker.\nTheir partnership wrote the template for Telugu pan-India films.",
            "hashtags": "#Prabhas #Rajamouli #Baahubali #Tollywood",
        },
        {
            "key":     "pb_13_before",
            "section": "overview",
            "hook":    "Prabhas had 13 films before Baahubali that most fans have never seen.",
            "body":    "He was consistent and underrated for a decade before the franchise.\nBaahubali didn't create him — it revealed him.",
            "hashtags": "#Prabhas #Baahubali #Tollywood",
        },
    ],

    # ── Dhanush ───────────────────────────────────────────────────────────────

    "Dhanush": [
        {
            "key":     "dh_directed_4",
            "section": "overview",
            "hook":    "Dhanush has directed 4 films. While being a top actor.",
            "body":    "Actor, director, and he does both at the highest level.\nNo one else on this list pulls that off.",
            "hashtags": "#Dhanush #Kollywood #SouthCinema",
        },
        {
            "key":     "dh_vetri_zero_failures",
            "section": "directors",
            "hook":    "Every Dhanush-Vetrimaaran film has been critically acclaimed.",
            "body":    "5 collaborations. Zero failures.\nThe most reliable duo in Tamil cinema — and nobody debates it.",
            "hashtags": "#Dhanush #Vetrimaaran #Kollywood",
        },
        {
            "key":     "dh_4_languages",
            "section": "overview",
            "hook":    "Dhanush has acted in Tamil, Telugu, Hindi, and Malayalam.",
            "body":    "The most cross-industry Tamil star of his generation.\nHis network reaches every corner of South India — and beyond.",
            "hashtags": "#Dhanush #Kollywood #SouthCinema",
        },
    ],

    # ── Yash ─────────────────────────────────────────────────────────────────

    "Yash": [
        {
            "key":     "yash_only_63",
            "section": "collaborators",
            "hook":    "Yash has worked with only 63 actors in his entire career.",
            "body":    "The smallest network on this list by far.\nYet his pan-India reach rivals actors with 10x the filmography.\nDepth beats breadth.",
            "hashtags": "#Yash #KGF #Sandalwood #SouthCinema",
        },
        {
            "key":     "yash_2_films",
            "section": "overview",
            "hook":    "Yash built pan-India stardom on essentially 2 films.",
            "body":    "KGF Chapter 1 and KGF Chapter 2. That is the entire case.\nHighest cultural ROI of any South Indian star this generation.",
            "hashtags": "#Yash #KGF #Sandalwood",
        },
    ],

    # ── Rakshit Shetty ────────────────────────────────────────────────────────

    "Rakshit Shetty": [
        {
            "key":     "rs_777_charlie",
            "section": "overview",
            "hook":    "777 Charlie made the whole country cry — regardless of language.",
            "body":    "A Kannada film. A dog. No pan-India star cast.\nJust pure emotion that crossed every language border.",
            "hashtags": "#RakshitShetty #777Charlie #Sandalwood #SouthCinema",
        },
    ],

}

# ── Slot schedule (IST) ───────────────────────────────────────────────────────
SLOT_HOURS      = [7, 10, 13, 16, 19, 22]
GENERATION_HOUR = 21

def get_facts(actor_db_name: str) -> list[dict]:
    return FACTS.get(actor_db_name, [])

def all_actors_with_facts() -> list[str]:
    return list(FACTS.keys())
