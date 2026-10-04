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
  "career"       → /actors/{slug}/chart minimal page (career chart SVG)
  "compare"      → /compare/{actor}-vs-{compare_with} page (needs compare_with field)

Emotional metadata fields (all optional — see FACT_DEFAULTS for defaults):
  pillar            str   Content category: nostalgia | debate | milestone | dynasty | comparison
  debate_potential  int   1-10  How likely this is to spark replies/discussion
  people_forget     bool  If True, broadcaster hints "most don't know this" in framing
  preferred_window  list  Slot hours (IST) this fact performs best at — None = any slot
  birthday_relevant bool  Flag for actor birthday posts
  min_gap_days      int   Minimum days before this fact can be reused (default 14)
"""

from __future__ import annotations

# ── Defaults for every optional metadata field ────────────────────────────────

FACT_DEFAULTS: dict = {
    "pillar":            "milestone",
    "debate_potential":  5,
    "people_forget":     False,
    "preferred_window":  None,      # None = any slot is fine
    "birthday_relevant": False,
    "min_gap_days":      14,
}

def get_enriched_fact(fact: dict) -> dict:
    """Return fact merged with FACT_DEFAULTS — backward-compatible.

    Callers can always access enriched["debate_potential"] etc. without
    checking whether the raw fact dict contains those keys.
    """
    return {**FACT_DEFAULTS, **fact}

# ── Fact library ──────────────────────────────────────────────────────────────

FACTS: dict[str, list[dict]] = {

    # ── Kamal Haasan ──────────────────────────────────────────────────────────

    "Kamal Haasan": [
        {
            "key":             "kh_vs_rajini",
            "section":         "compare",
            "compare_with":    "rajinikanth",
            "hook":    "Kamal Haasan has 54 MORE films than Rajinikanth.",
            "body":    "246 vs 192.\nYet the debate about who is bigger never ends.\nThat is the magic of Rajini.",
            "hashtags": "#KamalHaasan #Rajinikanth #Kollywood",
            # metadata
            "pillar":           "comparison",
            "debate_potential":  9,
            "preferred_window":  [19, 22],
            "min_gap_days":      21,
        },
        {
            "key":     "kh_70s_insane",
            "section": "overview",
            "hook":    "Kamal Haasan made 104 films in the 1970s alone.",
            "body":    "That single decade has more films than Allu Arjun's entire career.\n104 films. One decade. One man.",
            "hashtags": "#KamalHaasan #Kollywood #SouthCinema",
            # metadata
            "pillar":           "nostalgia",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [7, 10],
        },
        {
            "key":     "kh_65_years",
            "section": "overview",
            "hook":    "Kamal Haasan has been acting for 65 years.",
            "body":    "Started in 1960 as a child actor.\nStill headlining films in 2025.\nNo other South Indian actor has an active career this long.",
            "hashtags": "#KamalHaasan #Kollywood #SouthCinema",
            # metadata
            "pillar":            "milestone",
            "debate_potential":   5,
            "preferred_window":   [7, 10, 13],
            "birthday_relevant":  True,
            "min_gap_days":       30,
        },
        {
            "key":     "kh_rajini_costars",
            "section": "collaborators",
            "hook":    "Kamal Haasan and Rajinikanth were co-stars in 36 films.",
            "body":    "They shared the screen 36 times before becoming parallel legends.\nThe greatest rivalry in Indian cinema started as a friendship.",
            "hashtags": "#KamalHaasan #Rajinikanth #Kollywood",
            # metadata
            "pillar":           "dynasty",
            "debate_potential":  8,
            "people_forget":     True,
            "preferred_window":  [19, 22],
            "min_gap_days":      21,
        },
        {
            "key":     "kh_kb_29",
            "section": "director-loyalty",
            "hook":    "One director worked with Kamal Haasan 29 times.",
            "body":    "K. Balachander directed Kamal 29 times — the longest actor-director partnership in Tamil film history.\nAlmost 1 in every 8 Kamal films.",
            "hashtags": "#KamalHaasan #Kollywood",
            # metadata
            "pillar":           "dynasty",
            "debate_potential":  5,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "kh_659_collabs",
            "section": "collaborators",
            "hook":    "659 actors have worked with Kamal Haasan.",
            "body":    "The most connected actor in South Indian cinema — by a wide margin.\nMore than any other actor on this list.",
            "hashtags": "#KamalHaasan #Kollywood #SouthCinema",
            # metadata
            "pillar":           "milestone",
            "debate_potential":  4,
        },
        {
            "key":     "kh_vs_new_gen_combined",
            "section": "overview",
            "hook":    "Kamal Haasan has more films than Vijay, Dhanush, and Ajith Kumar combined.",
            "body":    "Vijay (75) + Dhanush (70) + Ajith (64) = 209.\nKamal Haasan: 233.\nThe three biggest Tamil stars of this generation — combined — still fall short.",
            "hashtags": "#KamalHaasan #Kollywood #SouthCinema",
            "pillar":           "comparison",
            "debate_potential":  8,
            "people_forget":     True,
            "preferred_window":  [19, 22],
            "min_gap_days":      21,
        },
    ],

    # ── Rajinikanth ───────────────────────────────────────────────────────────

    "Rajinikanth": [
        {
            "key":     "rj_85_in_80s",
            "section": "overview",
            "hook":    "Rajinikanth did 85 films in just the 1980s.",
            "body":    "That is nearly 9 films a year for a full decade.\nThe entire filmographies of Yash, Ram Charan, and Allu Arjun — combined — don't match that.",
            "hashtags": "#Rajinikanth #Superstar #Kollywood",
            # metadata
            "pillar":           "nostalgia",
            "debate_potential":  8,
            "people_forget":     True,
            "preferred_window":  [19, 22],
        },
        {
            "key":     "rj_kamal_36",
            "section": "collaborators",
            "hook":    "Rajinikanth and Kamal were co-stars in 35 films.",
            "body":    "Before they became icons, they were each other's screen partners.\n35 films together. Then two separate empires.",
            "hashtags": "#Rajinikanth #KamalHaasan #Kollywood",
            # metadata
            "pillar":           "dynasty",
            "debate_potential":  8,
            "people_forget":     True,
            "preferred_window":  [19, 22],
            "min_gap_days":      21,
        },
        {
            "key":     "rj_sp_24",
            "section": "director-loyalty",
            "hook":    "The same director made 24 films with Rajinikanth.",
            "body":    "S.P. Muthuraman directed him 24 times — that is 1 in every 8 Rajini films.\nMost fans have never heard this name.",
            "hashtags": "#Rajinikanth #Superstar #Kollywood",
            # metadata
            "pillar":           "dynasty",
            "debate_potential":  5,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "rj_dwarfs_new_gen",
            "section": "overview",
            "hook":    "Rajinikanth has more films than Ram Charan, Jr. NTR, Allu Arjun, and Prabhas combined.",
            "body":    "RC (20) + NTR (43) + AA (33) + Prabhas (25) = 121.\nRajinikanth: 192.\nThe four biggest Telugu stars of this generation, combined, still fall short.",
            "hashtags": "#Rajinikanth #Superstar #SouthCinema",
            # metadata
            "pillar":           "comparison",
            "debate_potential":  9,
            "people_forget":     True,
            "preferred_window":  [19, 22],
            "min_gap_days":      21,
        },
        {
            "key":     "rj_2_0_box_office",
            "section": "blockbusters",
            "hook":    "Rajinikanth's biggest box office hit is 2.0 — it earned ₹963 crore.",
            "body":    "Not Enthiran (₹521 Cr). Not Jailer (₹613 Cr). Not Kabali.\n2.0 with Akshay Kumar as the villain. The sequel beat the original by almost double.",
            "hashtags": "#Rajinikanth #Superstar #Kollywood",
            "pillar":           "milestone",
            "debate_potential":  7,
            "people_forget":     True,
            "preferred_window":  [19, 22],
        },
        {
            "key":     "rj_thyagarajan_9",
            "section": "directors",
            "hook":    "Rajinikanth's second-most frequent director — after S.P. Muthuraman (24 films) — directed him 9 times.",
            "body":    "R. Thyagarajan: 9 films with Rajini. The second-biggest partnership no one talks about.\nWhile everyone remembers the Superstar, these directors built the machine.",
            "hashtags": "#Rajinikanth #Superstar #Kollywood",
            "pillar":           "dynasty",
            "debate_potential":  5,
            "people_forget":     True,
            "preferred_window":  [13, 16],
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
            # metadata
            "pillar":           "comparison",
            "debate_potential":  9,
            "people_forget":     True,
            "preferred_window":  [19, 22],
            "min_gap_days":      21,
        },
        {
            "key":     "mm_80s_206",
            "section": "overview",
            "hook":    "Mammootty made 206 films in the 1980s alone.",
            "body":    "That is more than Ram Charan, Allu Arjun, Yash, and Jr. NTR's entire careers — combined.\nOne decade. 206 films.",
            "hashtags": "#Mammootty #Mollywood #SouthCinema",
            # metadata
            "pillar":           "nostalgia",
            "debate_potential":  7,
            "people_forget":     True,
            "preferred_window":  [19, 22],
        },
        {
            "key":     "mm_sukumari_104",
            "section": "collaborators",
            "hook":    "The same actress appeared in 104 of Mammootty's films.",
            "body":    "Sukumari starred alongside him in 104 films.\nThat single co-star count is more than Allu Arjun's entire filmography.",
            "hashtags": "#Mammootty #Mollywood",
            # metadata
            "pillar":           "nostalgia",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "mm_700",
            "section": "collaborators",
            "hook":    "700 actors have worked with Mammootty. 700.",
            "body":    "The widest collaboration network in South Indian cinema.\nIn Mollywood, everyone has a Mammootty connection.",
            "hashtags": "#Mammootty #Mammukka #Mollywood",
            # metadata
            "pillar":           "milestone",
            "debate_potential":  5,
        },
        {
            "key":     "mm_two_dirs_33",
            "section": "directors",
            "hook":    "Two different directors worked with Mammootty exactly 33 times each.",
            "body":    "Joshiy: 33 films. I.V. Sasi: 33 films.\nPerfect symmetry — no other actor in South India has this.",
            "hashtags": "#Mammootty #Mollywood",
            # metadata
            "pillar":           "dynasty",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "mm_vs_kamal",
            "section": "compare",
            "compare_with": "kamal-haasan",
            "hook":    "Mammootty has made 212 more films than Kamal Haasan.",
            "body":    "Mammootty: 444. Kamal Haasan: 233. Gap: 212 films.\nThat gap — 212 films — is bigger than Ajith Kumar's entire career.\nThe Mollywood grind makes even the Tamil legends look selective.",
            "hashtags": "#Mammootty #KamalHaasan #Mollywood #Kollywood",
            "pillar":           "comparison",
            "debate_potential":  8,
            "people_forget":     True,
            "preferred_window":  [19, 22],
            "min_gap_days":      21,
        },
        {
            "key":     "mm_sukumari_sathyan",
            "section": "directors",
            "hook":    "Sathyan Anthikad directed Mammootty 20 times — the same director who made 20 films with Mohanlal.",
            "body":    "Sathyan Anthikad: 20 films with Mammootty. 20 films with Mohanlal.\nThe most symmetrically successful director in Mollywood history — and he chose both legends equally.",
            "hashtags": "#Mammootty #Mollywood",
            "pillar":           "dynasty",
            "debate_potential":  7,
            "people_forget":     True,
            "preferred_window":  [16, 19],
        },
    ],

    # ── Mohanlal ──────────────────────────────────────────────────────────────

    "Mohanlal": [
        {
            "key":     "ml_priyan_36",
            "section": "director-loyalty",
            "hook":    "One director made 36 films with Mohanlal. 36.",
            "body":    "Priyadarshan directed him 36 times — the highest actor-director count on this list.\nThat partnership alone spans three decades.",
            "hashtags": "#Mohanlal #Lalettan #Mollywood",
            # metadata
            "pillar":           "dynasty",
            "debate_potential":  6,
            "preferred_window":  [16, 19],
        },
        {
            "key":     "ml_jagathy_114",
            "section": "collaborators",
            "hook":    "The same co-star appeared in 114 of Mohanlal's films.",
            "body":    "Jagathy Sreekumar: 114 films with Mohanlal.\nThat number is more than Allu Arjun's entire career.",
            "hashtags": "#Mohanlal #Mollywood",
            # metadata
            "pillar":           "nostalgia",
            "debate_potential":  7,
            "people_forget":     True,
            "preferred_window":  [19, 22],
        },
        {
            "key":     "ml_mamm_60",
            "section": "collaborators",
            "hook":    "Mohanlal and Mammootty shared the screen in 60 films.",
            "body":    "South India's two greatest parallel careers — and they appeared together 60 times.\nThe Mollywood golden era was built on this.",
            "hashtags": "#Mohanlal #Mammootty #Mollywood",
            # metadata
            "pillar":           "comparison",
            "debate_potential":  9,
            "preferred_window":  [19, 22],
            "min_gap_days":      21,
        },
        {
            "key":     "ml_80s_166",
            "section": "overview",
            "hook":    "Mohanlal made 166 films in just the 1980s.",
            "body":    "That is over 16 films a year for a decade.\nAnd somehow, each role was different.",
            "hashtags": "#Mohanlal #Lalettan #Mollywood",
            # metadata
            "pillar":           "nostalgia",
            "debate_potential":  7,
            "people_forget":     True,
            "preferred_window":  [19, 22],
        },
        {
            "key":     "ml_sathyan_20",
            "section": "directors",
            "hook":    "Sathyan Anthikad directed Mohanlal 20 times.",
            "body":    "The quiet genius of Malayalam family dramas — and the man responsible for 20 of Mohanlal's most beloved films.\nBehind every great actor is a director who understands him completely.",
            "hashtags": "#Mohanlal #Lalettan #Mollywood",
            "pillar":           "dynasty",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "ml_iv_sasi_22",
            "section": "directors",
            "hook":    "Before Priyadarshan's 36 films with Mohanlal, I.V. Sasi had already directed him 22 times.",
            "body":    "I.V. Sasi: 22 films. Priyadarshan: 36 films.\nTwo directors. 58 films between them.\nMohanlal has been directed more times by two men than most actors have total films.",
            "hashtags": "#Mohanlal #Lalettan #Mollywood",
            "pillar":           "dynasty",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
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
            # metadata
            "pillar":           "dynasty",
            "debate_potential":  7,
            "people_forget":     True,
            "preferred_window":  [13, 16, 19],
        },
        {
            "key":          "vj_ajith_343",
            "section":      "compare",
            "compare_with": "ajith-kumar",
            "hook":    "Vijay and Ajith have worked with the EXACT same number of actors. 343 each.",
            "body":    "Same era. Same industry. Same network depth.\nThe biggest rivalry in Tamil cinema is mathematically even.",
            "hashtags": "#Vijay #Ajith #Kollywood",
            # metadata
            "pillar":           "debate",
            "debate_potential":  10,
            "preferred_window":  [19, 22],
            "min_gap_days":      21,
        },
        {
            "key":     "vj_career_arc",
            "section": "overview",
            "hook":    "Vijay made 24 films before his first ₹100Cr hit. His last 5 averaged multiples of that.",
            "body":    "Two decades of grinding. Then a ceiling that kept moving.\nThe Vijay story is not a rocket launch — it's a slow burn that never stopped.",
            "hashtags": "#Vijay #Thalapathy #Kollywood",
            # metadata
            "pillar":           "milestone",
            "debate_potential":  7,
            "people_forget":     True,
            "preferred_window":  [13, 16, 19],
        },
        {
            "key":     "vj_vadivelu_14",
            "section": "collaborators",
            "hook":    "Vadivelu appeared in 14 Vijay films.",
            "body":    "14 films together — more than most leading actors work with a single co-star in a lifetime.\nAsk any 90s Tamil kid which scenes they remember.",
            "hashtags": "#Vijay #Vadivelu #Kollywood",
            # metadata
            "pillar":           "nostalgia",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16, 19],
        },
        {
            "key":     "vj_manivannan_17",
            "section": "collaborators",
            "hook":    "The most frequent co-star in Vijay's career is Manivannan — 17 films.",
            "body":    "More than Vadivelu (14). More than Nassar (13). More than Vivek (12).\nThe most undiscussed presence in Tamil cinema appeared in 17 Vijay films.",
            "hashtags": "#Vijay #Thalapathy #Kollywood",
            "pillar":           "nostalgia",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "vj_nassar_13",
            "section": "collaborators",
            "hook":    "Nassar appeared in 13 Vijay films.",
            "body":    "Tamil cinema's most reliable villain-character actor — and the man who showed up for Vijay 13 times.\nWhen the story needed a foil, the answer was always Nassar.",
            "hashtags": "#Vijay #Thalapathy #Nassar #Kollywood",
            "pillar":           "nostalgia",
            "debate_potential":  5,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "vj_75_last",
            "section": "overview",
            "hook":    "Vijay has 75 films. His next is reportedly his last before entering politics.",
            "body":    "From child actor to pan-India phenomenon — 75 films, one transition.\nThe goodbye is already in the data.",
            "hashtags": "#Vijay #Thalapathy #Kollywood",
            "pillar":           "milestone",
            "debate_potential":  8,
            "preferred_window":  [19, 22],
            "min_gap_days":      21,
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
            # metadata
            "pillar":           "debate",
            "debate_potential":  10,
            "preferred_window":  [19, 22],
            "min_gap_days":      21,
        },
        {
            "key":     "ak_solo_empire",
            "section": "overview",
            "hook":    "Ajith Kumar built a ₹5,000Cr+ career without ever relying on one director.",
            "body":    "No director has made more than 4 films with him.\nEverything he has, he built on his own terms.",
            "hashtags": "#Ajith #Thala #Kollywood",
            # metadata
            "pillar":           "milestone",
            "debate_potential":  7,
            "people_forget":     True,
            "preferred_window":  [13, 16, 19],
        },
        {
            "key":     "ak_no_loyalty",
            "section": "directors",
            "hook":    "No single director has worked with Ajith more than 4 times.",
            "body":    "Siva, Saran, H. Vinoth — all maxed at 4 films.\nHe has been equally fair to everyone. No one owns his career.",
            "hashtags": "#Ajith #Thala #Kollywood",
            # metadata
            "pillar":           "milestone",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "ak_debut_64",
            "section": "overview",
            "hook":    "Ajith Kumar debuted in 1990. He has made 64 films in 35 years. He is still going.",
            "body":    "No misfires that stopped his career. No controversy that broke his run.\nThe most consistent top Tamil star in history.",
            "hashtags": "#Ajith #Thala #Kollywood",
            "pillar":           "milestone",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16, 19],
        },
        {
            "key":     "ak_vivek_12",
            "section": "collaborators",
            "hook":    "The late comedian Vivek appeared in 12 Ajith Kumar films.",
            "body":    "12 films. The longest comedian-hero partnership in Ajith's career.\nEvery time Vivek was on screen, Ajith's world felt complete.",
            "hashtags": "#Ajith #Thala #Kollywood",
            "pillar":           "nostalgia",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "ak_saran_siva_symmetry",
            "section": "directors",
            "hook":    "Two directors have each worked with Ajith exactly 4 times — Saran and Siva.",
            "body":    "Perfect symmetry. Same count. Different eras.\nSaran built Thala in the 2000s. Siva rebuilt him in the 2010s.\nNo other Tamil star has two directors with an identical count like this.",
            "hashtags": "#Ajith #Thala #Kollywood",
            "pillar":           "dynasty",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
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
            # metadata
            "pillar":           "dynasty",
            "debate_potential":  8,
            "people_forget":     True,
            "preferred_window":  [13, 16, 19],
        },
        {
            "key":     "ntr_brahma_16",
            "section": "collaborators",
            "hook":    "Brahmanandam appears in 16 of Jr. NTR's 43 films.",
            "body":    "More than 1 in every 3 NTR films has the same comedian.\nThe most consistent actor-comedian duo in Tollywood.",
            "hashtags": "#JrNTR #Brahmanandam #Tollywood",
            # metadata
            "pillar":           "nostalgia",
            "debate_potential":  5,
        },
        {
            "key":     "ntr_child_1991",
            "section": "filmography",
            "hook":    "Jr. NTR has been acting since 1991. He was 8 years old.",
            "body":    "34 years in front of a camera. Over 40 films. Three generations of Telugu fans.\nRRR was not his arrival — it was his peak.",
            "hashtags": "#JrNTR #Tarak #Tollywood",
            "pillar":           "nostalgia",
            "debate_potential":  7,
            "people_forget":     True,
            "preferred_window":  [13, 16, 19],
        },
        {
            "key":     "ntr_comedian_trio",
            "section": "collaborators",
            "hook":    "Jr. NTR's 3 most frequent co-stars are all comedians.",
            "body":    "Brahmanandam: 16 films. Raghu Babu: 14. M. S. Narayana: 13.\n43 appearances across 3 comedians.\nNo Telugu hero made the audience laugh harder — through other people.",
            "hashtags": "#JrNTR #Brahmanandam #Tollywood",
            "pillar":           "nostalgia",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "ntr_2000s_23",
            "section": "overview",
            "hook":    "Jr. NTR made 23 films in just the 2000s.",
            "body":    "More than 2 films a year for an entire decade.\nBefore Bollywood knew his name, Tollywood already called him Tarak.",
            "hashtags": "#JrNTR #Tarak #Tollywood",
            "pillar":           "milestone",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [19, 22],
        },
        {
            "key":     "ntr_rc_3",
            "section": "collaborators",
            "hook":    "Ram Charan and Jr. NTR appeared in 3 films together before RRR.",
            "body":    "The brotherhood is not a PR campaign — it is a 3-film record.\nWhen Rajamouli put them in one frame, it felt inevitable.",
            "hashtags": "#JrNTR #RamCharan #RRR #Tollywood",
            "pillar":           "dynasty",
            "debate_potential":  8,
            "people_forget":     True,
            "preferred_window":  [19, 22],
            "min_gap_days":      21,
        },
        {
            "key":     "ntr_nassar_11",
            "section": "collaborators",
            "hook":    "Nassar appeared in 11 Jr. NTR films — crossing the Tamil-Telugu border for the same star.",
            "body":    "The Tamil character actor who kept showing up in Tollywood.\nFor NTR, Nassar was not a guest — he was a fixture.",
            "hashtags": "#JrNTR #Tarak #Tollywood #Kollywood",
            "pillar":           "nostalgia",
            "debate_potential":  5,
            "people_forget":     True,
            "preferred_window":  [13, 16],
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
            # metadata
            "pillar":           "milestone",
            "debate_potential":  7,
            "preferred_window":  [13, 16, 19],
        },
        {
            "key":     "rc_brahma_50pct",
            "section": "collaborators",
            "hook":    "Brahmanandam appears in exactly half of Ram Charan's films.",
            "body":    "10 out of 20 films. 50% of his entire career has the same comedian.\nThat is not coincidence. That is a formula.",
            "hashtags": "#RamCharan #Brahmanandam #Tollywood",
            # metadata
            "pillar":           "nostalgia",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "rc_ntr_5",
            "section": "collaborators",
            "hook":    "Ram Charan's most frequent co-star is Jr. NTR — 5 films together.",
            "body":    "Their bond goes way deeper than RRR.\nWhen the data says 5 films, the friendship finally makes sense.",
            "hashtags": "#RamCharan #JrNTR #RRR #Tollywood",
            # metadata
            "pillar":           "dynasty",
            "debate_potential":  7,
            "people_forget":     True,
            "preferred_window":  [19, 22],
        },
        {
            "key":     "rc_18_directors",
            "section": "directors",
            "hook":    "Ram Charan has worked with 18 different directors in just 22 films.",
            "body":    "The highest director-diversity ratio on this list.\nAlmost every film is a new creative bet.\nNo single director owns the Ram Charan story — he does.",
            "hashtags": "#RamCharan #Tollywood",
            "pillar":           "milestone",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "rc_chiranjeevi_4",
            "section": "collaborators",
            "hook":    "Chiranjeevi appeared in 4 of Ram Charan's films — including Acharya, where they played father and son.",
            "body":    "The only father-son pair in South Indian cinema to share that kind of on-screen count.\nThe Mega family did not just produce a career — it appeared in it.",
            "hashtags": "#RamCharan #Chiranjeevi #Tollywood",
            "pillar":           "dynasty",
            "debate_potential":  7,
            "people_forget":     True,
            "preferred_window":  [19, 22],
        },
        {
            "key":     "rc_rajamouli_3",
            "section": "directors",
            "hook":    "SS Rajamouli is Ram Charan's most frequent director — 3 films together.",
            "body":    "Magadheera. RRR. And one more.\nFor a star with 22 films and 18 directors, 3 with the same person is the clearest vote of confidence in the room.",
            "hashtags": "#RamCharan #Rajamouli #RRR #Tollywood",
            "pillar":           "dynasty",
            "debate_potential":  7,
            "preferred_window":  [16, 19],
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
            # metadata
            "pillar":           "dynasty",
            "debate_potential":  7,
            "preferred_window":  [16, 19],
        },
        {
            "key":     "aa_brahma_42pct",
            "section": "collaborators",
            "hook":    "Brahmanandam is in 42% of Allu Arjun's movies.",
            "body":    "14 out of 33 films.\nNearly half of Bunny's filmography has the same comedian in it.",
            "hashtags": "#AlluArjun #Brahmanandam #Tollywood",
            # metadata
            "pillar":           "nostalgia",
            "debate_potential":  5,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "aa_trivikram_3",
            "section": "directors",
            "hook":    "Trivikram Srinivas directed Allu Arjun 3 times — including Ala Vaikunthapurramuloo.",
            "body":    "3 films. One of them became the highest-grossing Telugu film before Pushpa.\nThe writer-director who shaped Bunny's mass image.",
            "hashtags": "#AlluArjun #Bunny #Tollywood",
            "pillar":           "dynasty",
            "debate_potential":  6,
            "preferred_window":  [16, 19],
        },
        {
            "key":     "aa_prakash_raj_7",
            "section": "collaborators",
            "hook":    "Prakash Raj appears in 7 of Allu Arjun's films.",
            "body":    "South India's most-cast villain — and 7 times, Bunny was on the other side.\nNo Telugu hero-villain partnership has lasted longer.",
            "hashtags": "#AlluArjun #PrakashRaj #Tollywood",
            "pillar":           "nostalgia",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "aa_rao_ramesh_10",
            "section": "collaborators",
            "hook":    "Rao Ramesh appears in 10 of Allu Arjun's films.",
            "body":    "Father, uncle, boss, villain — he has played every role opposite Bunny.\nThe character actor who quietly powered half the Allu Arjun filmography.",
            "hashtags": "#AlluArjun #Tollywood",
            "pillar":           "nostalgia",
            "debate_potential":  5,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "aa_fahadh_3",
            "section": "collaborators",
            "hook":    "Allu Arjun and Fahadh Faasil have shared the screen in 3 films — including Pushpa 2.",
            "body":    "Malayalam meets Telugu. The pan-South alliance the internet wanted — and the data confirms.\n3 films. The chemistry was always in the numbers.",
            "hashtags": "#AlluArjun #FahadhFaasil #Pushpa #SouthCinema",
            "pillar":           "comparison",
            "debate_potential":  7,
            "preferred_window":  [19, 22],
        },
        {
            "key":     "aa_chiru_3",
            "section": "collaborators",
            "hook":    "Chiranjeevi appeared in 3 films alongside his nephew Allu Arjun.",
            "body":    "The Mega family runs deep — 3 on-screen appearances between uncle and nephew.\nThe most powerful dynasty in Tollywood was never just off-screen.",
            "hashtags": "#AlluArjun #Chiranjeevi #Tollywood",
            "pillar":           "dynasty",
            "debate_potential":  7,
            "people_forget":     True,
            "preferred_window":  [16, 19],
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
            # metadata
            "pillar":           "dynasty",
            "debate_potential":  7,
            "preferred_window":  [16, 19],
        },
        {
            "key":     "pb_13_before",
            "section": "overview",
            "hook":    "Prabhas had 13 films before Baahubali that most fans have never seen.",
            "body":    "He was consistent and underrated for a decade before the franchise.\nBaahubali didn't create him — it revealed him.",
            "hashtags": "#Prabhas #Baahubali #Tollywood",
            # metadata
            "pillar":           "nostalgia",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "pb_anushka_5",
            "section": "collaborators",
            "hook":    "Anushka Shetty appears in 5 of Prabhas's films — including both Baahubali parts.",
            "body":    "From Chatrapathi (2005) to Baahubali 2 (2017) — the partnership that outlasted every other pairing in Telugu cinema.\nThe franchise had a face. It had two.",
            "hashtags": "#Prabhas #AnushkaShetty #Baahubali #Tollywood",
            "pillar":           "dynasty",
            "debate_potential":  7,
            "people_forget":     True,
            "preferred_window":  [16, 19],
        },
        {
            "key":     "pb_rana_5",
            "section": "collaborators",
            "hook":    "Rana Daggubati and Prabhas have appeared in 5 films together.",
            "body":    "The Baahubali brothers were already brothers in 4 other films before the franchise.\nThe on-screen rivalry was built on an off-screen bond.",
            "hashtags": "#Prabhas #RanaDaggubati #Baahubali #Tollywood",
            "pillar":           "nostalgia",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "pb_21_grind",
            "section": "overview",
            "hook":    "Prabhas had 21 films before Baahubali. Most people know none of them.",
            "body":    "2002 to 2014. 21 films. Consistent, underrated, loyal to Tollywood.\nBaahubali didn't launch him — it revealed what was already there.",
            "hashtags": "#Prabhas #Baahubali #Tollywood",
            "pillar":           "nostalgia",
            "debate_potential":  7,
            "people_forget":     True,
            "preferred_window":  [13, 16, 19],
        },
        {
            "key":     "pb_12_post_2020",
            "section": "overview",
            "hook":    "Post-Baahubali, Prabhas has made 12 more films in just 5 years.",
            "body":    "The most prolific phase of his career came AFTER his biggest hit.\nMost stars become selective after a blockbuster. Prabhas got busier.",
            "hashtags": "#Prabhas #Tollywood",
            "pillar":           "milestone",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "pb_prashanth_neel_2",
            "section": "directors",
            "hook":    "Prashanth Neel — the director of KGF — has directed Prabhas twice.",
            "body":    "The Salaar franchise handed KGF's director a new universe to build.\n2 films together. The biggest director-star alliance in South India continues to expand.",
            "hashtags": "#Prabhas #PrashanthNeel #Salaar #Tollywood",
            "pillar":           "dynasty",
            "debate_potential":  7,
            "preferred_window":  [16, 19],
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
            # metadata
            "pillar":           "milestone",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "dh_vetri_zero_failures",
            "section": "directors",
            "hook":    "Every Dhanush-Vetrimaaran film has been critically acclaimed.",
            "body":    "5 collaborations. Zero failures.\nThe most reliable duo in Tamil cinema — and nobody debates it.",
            "hashtags": "#Dhanush #Vetrimaaran #Kollywood",
            # metadata
            "pillar":           "dynasty",
            "debate_potential":  7,
            "preferred_window":  [16, 19],
        },
        {
            "key":     "dh_vetri_vs_rest",
            "section": "directors",
            "hook":    "Dhanush has worked with 30+ directors. Only one has a perfect record.",
            "body":    "5 films with Vetrimaaran. 5 critical successes. Zero commercial failures.\nEvery other director in his career has had at least one miss.",
            "hashtags": "#Dhanush #Vetrimaaran #Kollywood",
            # metadata
            "pillar":           "dynasty",
            "debate_potential":  8,
            "people_forget":     True,
            "preferred_window":  [16, 19],
        },
        {
            "key":     "dh_selvaraghavan_6",
            "section": "directors",
            "hook":    "Dhanush's own brother directed him 6 times.",
            "body":    "K. Selvaraghavan: Kadhal Kondein. Pudhupettai. Aadukalam. And 3 more.\nThe most personal director in Tamil cinema was his actual family.",
            "hashtags": "#Dhanush #Selvaraghavan #Kollywood",
            "pillar":           "dynasty",
            "debate_potential":  7,
            "people_forget":     True,
            "preferred_window":  [13, 16, 19],
        },
        {
            "key":     "dh_aanand_4",
            "section": "directors",
            "hook":    "Bollywood director Aanand L. Rai has directed Dhanush 4 times.",
            "body":    "No other South Indian actor has been directed this many times by the same Bollywood director.\nThe crossover that both industries keep coming back to.",
            "hashtags": "#Dhanush #AanandLRai #Kollywood",
            "pillar":           "comparison",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "dh_70_films",
            "section": "overview",
            "hook":    "Dhanush has made 70 films — while directing 4 of them himself.",
            "body":    "Tamil cinema's most relentless actor-director.\nEvery few films, he steps behind the camera. Every few films, the audience steps up in return.",
            "hashtags": "#Dhanush #Kollywood #SouthCinema",
            "pillar":           "milestone",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
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
            # metadata
            "pillar":           "milestone",
            "debate_potential":  7,
            "people_forget":     True,
            "preferred_window":  [16, 19],
        },
        {
            "key":     "yash_2_films",
            "section": "overview",
            "hook":    "Yash built pan-India stardom on essentially 2 films.",
            "body":    "KGF Chapter 1 and KGF Chapter 2. That is the entire case.\nHighest cultural ROI of any South Indian star this generation.",
            "hashtags": "#Yash #KGF #Sandalwood",
            # metadata
            "pillar":           "milestone",
            "debate_potential":  8,
            "preferred_window":  [19, 22],
        },
        {
            "key":     "yash_20_grind",
            "section": "filmography",
            "hook":    "Yash made 20 films over 11 years before KGF. Pan-India stardom came on film 21.",
            "body":    "Debut 2007. KGF: 2018. 11 years. 20 films. No pan-India release.\nThen one film changed everything — because 20 films of preparation were already done.",
            "hashtags": "#Yash #KGF #Sandalwood",
            "pillar":           "nostalgia",
            "debate_potential":  7,
            "people_forget":     True,
            "preferred_window":  [13, 16, 19],
        },
        {
            "key":     "yash_anant_nag_5",
            "section": "collaborators",
            "hook":    "Anant Nag appears in 5 of Yash's films — they go back 10 years before KGF.",
            "body":    "The senior actor who gave KGF its paternal weight had already done that job 4 times before.\nThe best directors cast who is ready. Not who is famous.",
            "hashtags": "#Yash #AnantNag #KGF #Sandalwood",
            "pillar":           "nostalgia",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "yash_prashanth_3",
            "section": "directors",
            "hook":    "Prashanth Neel directed Yash 3 times — before and including KGF.",
            "body":    "3 films together. KGF Chapter 1. KGF Chapter 2. And one before the franchise.\nThe Yash story IS the Prashanth Neel story — they built the empire together.",
            "hashtags": "#Yash #PrashanthNeel #KGF #Sandalwood",
            "pillar":           "dynasty",
            "debate_potential":  7,
            "preferred_window":  [16, 19],
        },
        {
            "key":     "yash_kgf2_759cr",
            "section": "blockbusters",
            "hook":    "KGF Chapter 2 earned ₹759 crore. Yash's second-biggest film before that earned ₹24 crore.",
            "body":    "Googly (2013): ₹24 crore. KGF Chapter 2 (2022): ₹759 crore.\nThe jump between his second-biggest and biggest film is larger than most careers total.\nThat is the KGF effect.",
            "hashtags": "#Yash #KGF #Sandalwood #SouthCinema",
            "pillar":           "milestone",
            "debate_potential":  8,
            "people_forget":     True,
            "preferred_window":  [19, 22],
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
            # metadata
            "pillar":           "nostalgia",
            "debate_potential":  6,
            "preferred_window":  [19, 22],
        },
        {
            "key":     "rs_hemanth_3",
            "section": "directors",
            "hook":    "The same director made Rakshit Shetty's two highest-rated films — and then came back for a third.",
            "body":    "Hemanth M. Rao: Godhi Banna Sadharana Mykattu (8.3). Sapta Sagaradaache Ello A (8.1). Sapta Sagaradaache Ello B (7.5).\n3 films. All certified. One director.",
            "hashtags": "#RakshitShetty #HemanthMRao #Sandalwood",
            "pillar":           "dynasty",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16, 19],
        },
        {
            "key":     "rs_sapta_both_parts",
            "section": "overview",
            "hook":    "Rakshit Shetty told one story in two films — rated 8.1 and 7.5 on IMDB.",
            "body":    "Sapta Sagaradaache Ello: Side A (8.1). Side B (7.5).\nA two-part Kannada romance that held the same emotional weight across both parts. A rare achievement.",
            "hashtags": "#RakshitShetty #SaptaSagaradaache #Sandalwood",
            "pillar":           "milestone",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [16, 19],
        },
        {
            "key":     "rs_directed_self",
            "section": "overview",
            "hook":    "Rakshit Shetty directed himself in 2 films. One is rated 7.7 on IMDB.",
            "body":    "Ulidavaru Kandanthe (2014): 7.7 IMDB. Directed by Rakshit. Starring Rakshit.\nIn Sandalwood, the man who wore every hat.",
            "hashtags": "#RakshitShetty #UlidavaruKandanthe #Sandalwood",
            "pillar":           "milestone",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
        {
            "key":     "rs_rishab_twice",
            "section": "directors",
            "hook":    "The director of Kantara directed Rakshit Shetty twice.",
            "body":    "Rishab Shetty directed Rakshit in Ricky (2016) and Kirik Party (2016).\nBefore Kantara changed everything, Rishab was already building something — with this man.",
            "hashtags": "#RakshitShetty #RishabShetty #Kantara #Sandalwood",
            "pillar":           "dynasty",
            "debate_potential":  7,
            "people_forget":     True,
            "preferred_window":  [16, 19],
        },
        {
            "key":     "rs_55_collabs",
            "section": "collaborators",
            "hook":    "Rakshit Shetty has worked with only 55 actors in his entire career.",
            "body":    "Smallest network on this list.\nYet his films have reached more cities, more languages, and more hearts than actors with triple the filmography.\nQuality over quantity.",
            "hashtags": "#RakshitShetty #Sandalwood",
            "pillar":           "milestone",
            "debate_potential":  6,
            "people_forget":     True,
            "preferred_window":  [13, 16],
        },
    ],

}

# ── Slot schedule (IST) ───────────────────────────────────────────────────────
# One tweet a day for now (7 PM IST prime time). Add hours back to this list to
# increase daily volume — the generator, planner and slot posters all key off it.
SLOT_HOURS      = [19]
GENERATION_HOUR = 21

def get_facts(actor_db_name: str) -> list[dict]:
    return FACTS.get(actor_db_name, [])

def all_actors_with_facts() -> list[str]:
    return list(FACTS.keys())
