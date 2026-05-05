ACTORS: list[dict] = [
    # Telugu
    {"name": "Mahesh Babu",        "db_name": "Mahesh Babu",          "handle": "urstrulyMahesh",  "industry": "Telugu"},
    {"name": "Allu Arjun",         "db_name": "Allu Arjun",           "handle": "alluarjun",       "industry": "Telugu"},
    {"name": "Jr. NTR",            "db_name": "Jr. NTR",              "handle": "tarak9999",       "industry": "Telugu"},
    {"name": "Ram Charan",         "db_name": "Ram Charan",           "handle": "AlwaysRamCharan", "industry": "Telugu"},
    {"name": "Pawan Kalyan",       "db_name": "Pawan Kalyan",         "handle": "PawanKalyan",     "industry": "Telugu"},
    {"name": "Vijay Deverakonda",  "db_name": "Vijay Deverakonda",    "handle": "TheDeverakonda",  "industry": "Telugu"},
    {"name": "Nani",               "db_name": "Nani",                 "handle": "NameisNani",      "industry": "Telugu"},
    {"name": "Nagarjuna",          "db_name": "Nagarjuna Akkineni",   "handle": "iamnagarjuna",    "industry": "Telugu"},
    {"name": "Chiranjeevi",        "db_name": "Chiranjeevi",          "handle": "KChiruTweets",    "industry": "Telugu"},
    {"name": "Rana Daggubati",     "db_name": "Rana Daggubati",       "handle": "RanaDaggubati",   "industry": "Telugu"},
    {"name": "Ravi Teja",          "db_name": "Ravi Teja",            "handle": "RaviTeja_offl",   "industry": "Telugu"},
    # Tamil
    {"name": "Kamal Haasan",       "db_name": "Kamal Haasan",         "handle": "ikamalhaasan",    "industry": "Tamil"},
    {"name": "Dhanush",            "db_name": "Dhanush",              "handle": "dhanushkraja",    "industry": "Tamil"},
    {"name": "Suriya",             "db_name": "Suriya",               "handle": "Suriya_offl",     "industry": "Tamil"},
    {"name": "Sivakarthikeyan",    "db_name": "Sivakarthikeyan",      "handle": "Siva_Kartikeyan", "industry": "Tamil"},
    {"name": "Silambarasan",       "db_name": "Silambarasan",         "handle": "str",             "industry": "Tamil"},
    {"name": "Vikram",             "db_name": "Vikram",               "handle": "chiyaan",         "industry": "Tamil"},
    {"name": "Vijay Sethupathi",   "db_name": "Vijay Sethupathi",     "handle": "VijaySethuOffl",  "industry": "Tamil"},
    {"name": "R. Madhavan",        "db_name": "R. Madhavan",          "handle": "ActorMadhavan",   "industry": "Tamil"},
    # Malayalam
    {"name": "Mohanlal",           "db_name": "Mohanlal",             "handle": "Mohanlal",        "industry": "Malayalam"},
    {"name": "Mammootty",          "db_name": "Mammootty",            "handle": "mammukka",        "industry": "Malayalam"},
    {"name": "Dulquer Salmaan",    "db_name": "Dulquer Salmaan",      "handle": "dulQuer",         "industry": "Malayalam"},
    {"name": "Prithviraj",         "db_name": "Prithviraj Sukumaran", "handle": "PrithviOfficial", "industry": "Malayalam"},
    {"name": "Tovino Thomas",      "db_name": "Tovino Thomas",        "handle": "ttovino",         "industry": "Malayalam"},
    {"name": "Nivin Pauly",        "db_name": "Nivin Pauly",          "handle": "NivinOfficial",   "industry": "Malayalam"},
    # Kannada
    {"name": "Yash",               "db_name": "Yash",                 "handle": "TheNameIsYash",   "industry": "Kannada"},
    {"name": "Sudeep",             "db_name": "Sudeep",               "handle": "KicchaSudeep",    "industry": "Kannada"},
    {"name": "Upendra",            "db_name": "Upendra",              "handle": "nimmaupendra",    "industry": "Kannada"},
    {"name": "Darshan",            "db_name": "Darshan",              "handle": "dasadarshan",     "industry": "Kannada"},
    {"name": "Rakshit Shetty",     "db_name": "Rakshit Shetty",       "handle": "rakshitshetty",   "industry": "Kannada"},
]

# Tier 2 — signal accounts (directors, composers, trade analysts).
# When these accounts tweet mentioning an actor, we find that actor's recent tweet
# and reply to IT (never to the signal account directly).
SIGNAL_ACCOUNTS: list[dict] = [
    # Directors
    {"name": "SS Rajamouli",       "handle": "ssrajamouli",      "role": "director"},
    {"name": "Shankar",            "handle": "shankarshanmugh",  "role": "director"},
    {"name": "Sukumar",            "handle": "SukumarWritings",  "role": "director"},
    {"name": "Trivikram Srinivas", "handle": "trivikram_s",      "role": "director"},
    {"name": "Lokesh Kanagaraj",   "handle": "Dir_Lokesh",       "role": "director"},
    {"name": "Vignesh Shivan",     "handle": "VigneshShivN",     "role": "director"},
    {"name": "Priyadarshan",       "handle": "priyadarshan_i",   "role": "director"},
    # Composers
    {"name": "AR Rahman",          "handle": "arrahman",         "role": "composer"},
    {"name": "Anirudh Ravichander","handle": "anirudhofficial",  "role": "composer"},
    {"name": "Thaman S",           "handle": "MusicThaman",      "role": "composer"},
    {"name": "Devi Sri Prasad",    "handle": "ThisIsDSP",        "role": "composer"},
    # Trade analysts
    {"name": "Ramesh Bala",        "handle": "rameshlaus",       "role": "trade"},
    {"name": "Manobala V",         "handle": "ManobalaV",        "role": "trade"},
    {"name": "Sreedhar Pillai",    "handle": "sri50",            "role": "trade"},
    {"name": "Kaushik LM",         "handle": "LMKMovieManiac",   "role": "trade"},
    {"name": "Umair Sandhu",       "handle": "UmairSandhu",      "role": "trade"},
]

# Quick lookups
BY_HANDLE: dict[str, dict]        = {a["handle"].lower(): a for a in ACTORS}
BY_DB_NAME: dict[str, dict]       = {a["db_name"].lower(): a for a in ACTORS}
ALL_HANDLES: list[str]            = [a["handle"] for a in ACTORS]

SIGNALS_BY_HANDLE: dict[str, dict] = {s["handle"].lower(): s for s in SIGNAL_ACCOUNTS}
ALL_SIGNAL_HANDLES: list[str]      = [s["handle"] for s in SIGNAL_ACCOUNTS]
