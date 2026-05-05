import httpx
from config import CINETRACE_BASE_URL

SENSITIVE_WORDS = [
    "death", "died", "passed away", "rip", "condolence",
    "family", "wife", "husband", "son", "daughter", "mother", "father",
    "politics", "party", "election", "vote", "government",
    "rival", "versus", "fight", "controversy", "arrest", "case",
    "salary", "money", "fee", "income", "net worth",
]

MAX_TWEET_LENGTH = 260

class ValidationResult:
    def __init__(self):
        self.passed      = True
        self.failures: list[str] = []
        self.penalty     = 0

    def fail(self, reason: str, penalty: int = 20):
        self.failures.append(reason)
        self.penalty += penalty

    def final_confidence(self, base: float) -> float:
        return max(0.0, base - self.penalty)

async def validate(reply_text: str, stat_used: str,
                   base_confidence: float, profile_url: str) -> tuple[bool, float, list[str]]:
    result = ValidationResult()

    # 1. Length check
    if len(reply_text) > MAX_TWEET_LENGTH:
        result.fail(f"Too long: {len(reply_text)} chars (max {MAX_TWEET_LENGTH})", penalty=30)

    # 2. Link presence check
    if CINETRACE_BASE_URL not in reply_text and "cinetrace.in" not in reply_text:
        result.fail("Missing cinetrace.in link", penalty=25)

    # 3. Sensitivity scan
    text_lower = reply_text.lower()
    for word in SENSITIVE_WORDS:
        if word in text_lower:
            result.fail(f"Sensitive word detected: '{word}'", penalty=40)
            break

    # 4. Tone checks — red flags
    red_flags = ["amazing", "incredible", "legend", "goat", "greatest",
                 "best actor", "love you", "fan", "fanboy", "#", "🙏"]
    for flag in red_flags:
        if flag.lower() in text_lower:
            result.fail(f"Tone flag: '{flag}'", penalty=15)

    # 5. Stat presence — reply must have a number
    has_number = any(ch.isdigit() for ch in reply_text)
    if not has_number:
        result.fail("No numeric stat found in reply", penalty=20)

    # 6. Link reachability (best-effort)
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.head(profile_url)
            if r.status_code >= 400:
                result.fail(f"Profile URL returned {r.status_code}", penalty=15)
    except Exception:
        result.fail("Profile URL unreachable", penalty=10)

    final_conf = result.final_confidence(base_confidence)
    passed     = len([f for f in result.failures if "Sensitive" in f or "Too long" in f]) == 0

    return passed, final_conf, result.failures
