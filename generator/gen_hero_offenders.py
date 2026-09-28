"""Generate superhero-themed mock offender-management-system data for pipeline testing.

Usage (command line): python gen_hero_offenders.py [rows] [output.csv] [seed] [answer_key.csv]
Usage (notebook):     from gen_hero_offenders import generate; generate(100_000, "data.csv")

Everything is fictional and deliberately dirty:
  - dates in 9 string formats, mixed within every date column, including two-digit years
  - out-of-range dates: ancient DOBs (pre-1677, beyond pandas' default timestamp range) and a future DOB
  - true nulls and whitespace-only nulls in every column except tdcj_number, plus "NONE" as a text token
  - mixed-case names, apostrophes, hyphens, multi-word surnames, generational suffixes, mononyms
  - multi-valued superpower field (semicolon-delimited) and offense text containing commas
  - repeat offenders: same SID and name across records with different TDCJ numbers
  - alias collisions: several people legitimately share an alias, plus copycats using famous ones
  v2 columns (for type-conversion and text sessions):
  - intake_time in 7 formats plus a "99:99" unknown sentinel
  - disciplinary_points as "3", "03", "3.0", "3 pts", "three", or "N/A"
  - restitution_owed as "$1,234.50", "USD 1,234.50", "(12.00)" credits, "-" for zero, etc.
  - escape_risk as Y/N/Yes/No/1/0/TRUE/FALSE/T/F/X, plus "U"/"UNK" for unknown
  - protective_custody: single-character Y/N from the legacy system mixed with T/F from the newer one,
    some lowercase, "U" for unknown
  - dampener_required: single-character T/F, some lowercase, rare "?"
  - conduct_notes: free-text officer notes; true sentiment is written to a separate answer key
  v4 coded domains (for crosswalks to reference data):
  - race: TDCJ-style letters mixed with words, abbreviations, legacy combined values (W/H, API) and
    values the old codes can't express (Middle Eastern, Multiracial); a few people's race differs across records
  - religion: many spellings and abbreviations of each faith group (RC, SBC, NOI, LDS), NONE vs DECLINED vs UNK
  - stg_affiliation: fictional villain groups with abbreviations and the status (confirmed, suspected, former)
    written into the same field; blank vs NONE; a few two-group cells
  - offense_code: FBI NIBRS offense codes with case, separator and Excel damage (13a, 13-A, 290.0, 9B for 09B),
    descriptions typed into the code field, invalid codes, and a few valid codes that contradict the violence code
  - violence_code: NV/V1/V2/V3 plus v1, V-1, bare digits, NON-VIOLENT and a few unmappable values
  - names: accented Hispanic surnames (GARCÍA vs GARCIA) and some mojibake (MUÃ‘OZ)
  - conduct_notes: web-form and encoding damage (<br>, &nbsp;, double spaces, curly quotes, â€™)
  Race, religion and STG are assigned at random, independently of every other column: no pattern in them
  means anything.
"""
import calendar
import csv
import io
import random
import sys
from datetime import date, timedelta

DEFAULT_ROWS = 100_000
DEFAULT_OUT = "hero_offender_data.csv"
DEFAULT_SEED = 2026

TRUE_NULL_RATE = 0.015
PADDED_NULL_RATE = 0.015
COPYCAT_RATE = 0.01          # procedural personas that use a famous alias
TODAY = date(2026, 9, 24)

rng = random.Random(DEFAULT_SEED)
case_rng = random.Random(DEFAULT_SEED + 1)   # name casing
v2_rng = random.Random(DEFAULT_SEED + 2)     # v2 columns; separate so v1 columns never change
v3_rng = random.Random(DEFAULT_SEED + 3)     # single-character flags; separate so earlier columns never change
v4_rng = random.Random(DEFAULT_SEED + 4)     # coded domains and text damage; separate so earlier values never change

HEADER = ["tdcj_number", "sid_number", "last_name", "first_name", "alias", "date_of_birth", "gender",
          "unit_location", "custody_level", "violence_code", "threat_level", "superpower",
          "primary_offense", "housing_restriction", "sentence_date", "projected_release_date"]
HEADER_V2 = ["intake_time", "disciplinary_points", "restitution_owed", "escape_risk",
             "protective_custody", "dampener_required", "conduct_notes"]
# Column order in the file: the v4 domain columns sit next to the columns they belong with.
HEADER_OUT = ["tdcj_number", "sid_number", "last_name", "first_name", "alias", "date_of_birth", "gender",
              "race", "religion", "unit_location", "custody_level", "violence_code", "threat_level",
              "stg_affiliation", "superpower", "primary_offense", "offense_code", "housing_restriction",
              "sentence_date", "projected_release_date", "intake_time", "disciplinary_points", "restitution_owed",
              "escape_risk", "protective_custody", "dampener_required", "conduct_notes"]

# ---------------------------------------------------------------------------------------------
# The 10 rows reviewed earlier, kept verbatim at the top of the file
# ---------------------------------------------------------------------------------------------
REVIEWED_CSV = """\
02381457,07845123,Kent,CLARK,SUPERMAN,06/18/1978,M,Metropolis,G2,V1,OMEGA,FLIGHT;SUPER STRENGTH;HEAT VISION;INVULNERABILITY,RECKLESS DAMAGE BY HEAT VISION >=$300K,KRYPTONITE-LINED CELL,2019-06-11,20310611
02290318,06912047,WAYNE,bruce,BATMAN,2/19/64,M,Gotham City,G3,V2,DELTA,GENIUS INTELLECT;MARTIAL ARTS;UNLIMITED BUDGET,VIGILANTISM W/GRAPPLING DEVICE,    ,14-MAR-2017,03/14/2027
02417782,08123390,pArKeR,Peter,SPIDER-MAN,8/10/01,M,Queens,   ,NV,BETA,WALL-CRAWLING;SPIDER-SENSE;WEB-SLINGING,OBSTRUCT HWY W/WEBBING,ADHESIVE-RESISTANT CELL,"August 22, 2022",Mar 30 2027
02155906,05678214,prince,DIANA,WONDER WOMAN,1213-03-22,F,Themyscira,G2,V2,ALPHA,SUPER STRENGTH;FLIGHT;LASSO OF TRUTH,COERCED CONFESSION W/LASSO,LASSO AND BRACELETS IN PROPERTY,20210117,2027-01-17 00:00:00
02398841,07992156,Allen,bArRy,THE FLASH,"March 14, 1989",M,Central City,G4,NV,ALPHA,SUPER SPEED;TIME TRAVEL,SPEEDING >=700 MPH IN SCHOOL ZONE,SPEED-DAMPENING COLLAR,09-30-2021,Jan 12 2028
02264470,06543987,HOWLETT,James,WOLVERINE,,M,Westchester,G5,V3,BETA,HEALING FACTOR;ADAMANTIUM CLAWS;ENHANCED SENSES,AGG ASSAULT W/ADAMANTIUM CLAWS,WALK-THROUGH METAL DETECTOR EXEMPT,11/03/2016,2029-11-03
02433015,07210448,munroe,ORORO,STORM,19851029,F,Cairo,G3,V1,OMEGA,WEATHER CONTROL;FLIGHT,CRIM MISCHIEF BY WEATHER EVENT >=$150K<$300K,INTERIOR CELL NO SKY ACCESS,2025-08-04,04-AUG-2030
02201583,06102775,bAnNeR,bruce,HULK,Dec 18 1975,M,Dayton,G5,V1,OMEGA,SUPER STRENGTH;GAMMA REGENERATION,"CRIM MISCHIEF >=$300K, HABITUAL",GAMMA-SHIELDED CELL,6/15/18,12/31/99
02347729,07456601,JORDAN,Hal,GREEN LANTERN,1979-02-20 00:00:00,M,Coast City,G3,NV,ALPHA,ENERGY CONSTRUCTS (POWER RING),UNL POSS PROHIBITED WEAPON (POWER RING),YELLOW-PAINTED CELL,"July 1, 2020",07-01-2028
02120394,05233118,T'Challa,,BLACK PANTHER,11-23-1983,M,Birnin Zana,G2,NV,BETA,ENHANCED STRENGTH;VIBRANIUM SUIT,UNAUTH ENTRY RESTRICTED AIRSPACE,VIBRANIUM SUIT IN PROPERTY,Feb 6 2024,06-FEB-2029
"""
REVIEWED = list(csv.reader(io.StringIO(REVIEWED_CSV)))

# ---------------------------------------------------------------------------------------------
# Famous seed pool: alias|last|first|gender|home city|powers|threat|fixed DOB (optional, ISO)
# Several aliases are deliberately shared by different people (e.g. five Green Lanterns).
# ---------------------------------------------------------------------------------------------
FAMOUS = """\
SUPERMAN|KENT|CLARK|M|Metropolis|FLIGHT;SUPER STRENGTH;HEAT VISION;INVULNERABILITY|OMEGA|
BATMAN|WAYNE|BRUCE|M|Gotham City|GENIUS INTELLECT;MARTIAL ARTS;UNLIMITED BUDGET|DELTA|
WONDER WOMAN|PRINCE|DIANA|F|Themyscira|SUPER STRENGTH;FLIGHT;LASSO OF TRUTH|ALPHA|1213-03-22
THE FLASH|ALLEN|BARRY|M|Central City|SUPER SPEED;TIME TRAVEL|ALPHA|
THE FLASH|WEST|WALLY|M|Keystone City|SUPER SPEED|ALPHA|
THE FLASH|GARRICK|JAY|M|Keystone City|SUPER SPEED|ALPHA|
GREEN LANTERN|JORDAN|HAL|M|Coast City|ENERGY CONSTRUCTS (POWER RING)|ALPHA|
GREEN LANTERN|STEWART|JOHN|M|Detroit|ENERGY CONSTRUCTS (POWER RING)|ALPHA|
GREEN LANTERN|GARDNER|GUY|M|Baltimore|ENERGY CONSTRUCTS (POWER RING)|ALPHA|
GREEN LANTERN|CRUZ|JESSICA|F|Portland|ENERGY CONSTRUCTS (POWER RING)|ALPHA|
GREEN LANTERN|SCOTT|ALAN|M|Gotham City|ENERGY CONSTRUCTS (POWER RING)|ALPHA|
AQUAMAN|CURRY|ARTHUR|M|Amnesty Bay|UNDERWATER BREATHING;MARINE TELEPATHY;SUPER STRENGTH|ALPHA|
MERA|CURRY|MERA|F|Xebel|HYDROKINESIS|ALPHA|
MARTIAN MANHUNTER|J'ONZZ|J'ONN|M|Middleton|SHAPESHIFTING;TELEPATHY;FLIGHT;PHASING|OMEGA|
CYBORG|STONE|VICTOR|M|Detroit|CYBERNETIC BODY;TECHNOPATHY|ALPHA|
GREEN ARROW|QUEEN|OLIVER|M|Star City|MASTER ARCHERY;MARTIAL ARTS|DELTA|
BLACK CANARY|LANCE|DINAH|F|Gotham City|SONIC SCREAM;MARTIAL ARTS|BETA|
SUPERGIRL|ZOR-EL|KARA|F|National City|FLIGHT;SUPER STRENGTH;HEAT VISION|OMEGA|
POWER GIRL|STARR|KAREN|F|New York|FLIGHT;SUPER STRENGTH;INVULNERABILITY|OMEGA|
NIGHTWING|GRAYSON|DICK|M|Bludhaven|ACROBATICS;MARTIAL ARTS|DELTA|
BATGIRL|GORDON|BARBARA|F|Gotham City|MARTIAL ARTS;HACKING|DELTA|
BATGIRL|CAIN|CASSANDRA|F|Gotham City|MARTIAL ARTS|DELTA|
BATWOMAN|KANE|KATE|F|Gotham City|MARTIAL ARTS;GADGETS|DELTA|
HUNTRESS|BERTINELLI|HELENA|F|Gotham City|MARTIAL ARTS;CROSSBOW|DELTA|
HAWKGIRL|HALL|SHIERA|F|St. Roch|FLIGHT (WINGS);NTH METAL MACE|BETA|
HAWKMAN|HALL|CARTER|M|St. Roch|FLIGHT (WINGS);NTH METAL MACE|BETA|
ZATANNA|ZATARA|ZATANNA|F|San Francisco|BACKWARDS SPELLCASTING|ALPHA|
CONSTANTINE|CONSTANTINE|JOHN|M|Liverpool|OCCULT MAGIC|ALPHA|
BLUE BEETLE|REYES|JAIME|M|El Paso|SCARAB ARMOR|ALPHA|
BLUE BEETLE|KORD|TED|M|Chicago|GADGETS;GENIUS INTELLECT|DELTA|
BOOSTER GOLD|CARTER|MICHAEL|M|Metropolis|POWERED ARMOR;FORCE FIELDS|BETA|
FIRESTORM|RAYMOND|RONNIE|M|Pittsburgh|MATTER MANIPULATION;FLIGHT|OMEGA|
THE ATOM|PALMER|RAY|M|Ivy Town|SIZE MANIPULATION|BETA|
PLASTIC MAN|O'BRIAN|PATRICK|M|Chicago|ELASTICITY;SHAPESHIFTING|ALPHA|
ELONGATED MAN|DIBNY|RALPH|M|Central City|ELASTICITY|BETA|
STARFIRE|ANDERS|KORY|F|Jump City|FLIGHT;ENERGY PROJECTION|ALPHA|
RAVEN|ROTH|RACHEL|F|Jump City|TELEPORTATION;CHAOS MAGIC|OMEGA|
VIXEN|MCCABE|MARI|F|Detroit|ANIMAL MIMICRY|BETA|
BLACK LIGHTNING|PIERCE|JEFFERSON|M|Freeland|ELECTRICITY MANIPULATION|ALPHA|
MISTER MIRACLE|FREE|SCOTT|M|Metropolis|ESCAPE ARTISTRY;GADGETS|BETA|
BIG BARDA|FREE|BARDA|F|Metropolis|SUPER STRENGTH;MEGA-ROD|ALPHA|
SWAMP THING|HOLLAND|ALEC|M|Houma|PLANT CONTROL;HEALING FACTOR|OMEGA|
ANIMAL MAN|BAKER|BUDDY|M|San Diego|ANIMAL MIMICRY|BETA|
DOCTOR FATE|NELSON|KENT|M|Salem|SORCERY|OMEGA|
METAMORPHO|MASON|REX|M|Chicago|MATTER MANIPULATION|ALPHA|
KATANA|YAMASHIRO|TATSU|F|Tokyo|SWORDSMANSHIP|DELTA|
DOCTOR MID-NITE|MCNIDER|CHARLES|M|Dayton|DETECTIVE SKILLS;NIGHT VISION|DELTA|
WILDCAT|GRANT|TED|M|Gotham City|MARTIAL ARTS|DELTA|
HOURMAN|TYLER|REX|M|Pittsburgh|SUPER STRENGTH|BETA|
SANDMAN|DODDS|WESLEY|M|New York|GADGETS;DETECTIVE SKILLS|DELTA|
STARMAN|KNIGHT|JACK|M|Opal City|ENERGY PROJECTION;FLIGHT|ALPHA|
MISTER TERRIFIC|HOLT|MICHAEL|M|Calvin City|GENIUS INTELLECT;GADGETS|DELTA|
JADE|HAYDEN|JENNIE-LYNN|F|Milwaukee|ENERGY CONSTRUCTS (POWER RING)|ALPHA|
THE QUESTION|SAGE|VIC|M|Hub City|DETECTIVE SKILLS;MARTIAL ARTS|DELTA|
CAPTAIN ATOM|ADAM|NATHANIEL|M|Deerwood|ENERGY PROJECTION;FLIGHT|OMEGA|
FIRE|DA COSTA|BEATRIZ|F|Rio de Janeiro|PYROKINESIS;FLIGHT|ALPHA|
ICE|OLAFSDOTTER|TORA|F|Oslo|CRYOKINESIS|ALPHA|
ROBOTMAN|STEELE|CLIFF|M|Midway City|CYBERNETIC BODY;SUPER STRENGTH|BETA|
NEGATIVE MAN|TRAINOR|LARRY|M|Midway City|ENERGY PROJECTION;FLIGHT|ALPHA|
ELASTI-WOMAN|FARR|RITA|F|Midway City|SIZE MANIPULATION|BETA|
SPIDER-MAN|PARKER|PETER|M|Queens|WALL-CRAWLING;SPIDER-SENSE;WEB-SLINGING|BETA|
SPIDER-MAN 2099|O'HARA|MIGUEL|M|Nueva York|WALL-CRAWLING;SUPER AGILITY|BETA|2071-06-06
SPIDER-WOMAN|DREW|JESSICA|F|London|ENERGY PROJECTION;FLIGHT|BETA|
IRON MAN|STARK|TONY|M|Manhattan|POWERED ARMOR;GENIUS INTELLECT|ALPHA|
WAR MACHINE|RHODES|JAMES|M|Philadelphia|POWERED ARMOR|ALPHA|
CAPTAIN AMERICA|ROGERS|STEVE|M|Brooklyn|SUPER SOLDIER SERUM;VIBRANIUM SHIELD|BETA|1918-07-04
WINTER SOLDIER|BARNES|JAMES|M|Brooklyn|CYBERNETIC BODY;SUPER SOLDIER SERUM|BETA|1917-03-10
FALCON|WILSON|SAM|M|Harlem|FLIGHT (WINGS);GADGETS|DELTA|
THOR|ODINSON|THOR|M|Asgard|FLIGHT;SUPER STRENGTH;ELECTRICITY MANIPULATION|OMEGA|0512-05-05
VALKYRIE|BRUNNHILDE||F|New Asgard|SWORDSMANSHIP;SUPER STRENGTH|ALPHA|
HULK|BANNER|BRUCE|M|Dayton|SUPER STRENGTH;GAMMA REGENERATION|OMEGA|
SHE-HULK|WALTERS|JENNIFER|F|Los Angeles|SUPER STRENGTH;INVULNERABILITY|ALPHA|
BLACK WIDOW|ROMANOFF|NATASHA|F|Volgograd|ESPIONAGE;MARTIAL ARTS|DELTA|
HAWKEYE|BARTON|CLINT|M|Waverly|MASTER ARCHERY|DELTA|
HAWKEYE|BISHOP|KATE|F|Manhattan|MASTER ARCHERY|DELTA|
MOCKINGBIRD|MORSE|BARBARA|F|Coronado|ESPIONAGE;MARTIAL ARTS|DELTA|
SCARLET WITCH|MAXIMOFF|WANDA|F|Westview|CHAOS MAGIC;REALITY WARPING|OMEGA|
QUICKSILVER|MAXIMOFF|PIETRO|M|Novi Grad|SUPER SPEED|ALPHA|
ANT-MAN|LANG|SCOTT|M|San Francisco|SIZE MANIPULATION|BETA|
THE WASP|VAN DYNE|JANET|F|Cresskill|SIZE MANIPULATION;FLIGHT|BETA|
DOCTOR STRANGE|STRANGE|STEPHEN|M|Greenwich Village|SORCERY;TELEPORTATION|OMEGA|
CAPTAIN MARVEL|DANVERS|CAROL|F|Boston|FLIGHT;ENERGY PROJECTION;SUPER STRENGTH|OMEGA|
SPECTRUM|RAMBEAU|MONICA|F|New Orleans|ENERGY PROJECTION;FLIGHT|OMEGA|
BLACK PANTHER|T'CHALLA||M|Birnin Zana|ENHANCED STRENGTH;VIBRANIUM SUIT|BETA|
DAREDEVIL|MURDOCK|MATT|M|Hell's Kitchen|ENHANCED SENSES;MARTIAL ARTS|DELTA|
LUKE CAGE|CAGE|LUKE|M|Harlem|INVULNERABILITY;SUPER STRENGTH|BETA|
IRON FIST|RAND|DANNY|M|K'un-Lun|MARTIAL ARTS;ENERGY PROJECTION|BETA|
JESSICA JONES|JONES|JESSICA|F|Hell's Kitchen|SUPER STRENGTH|BETA|
WOLVERINE|HOWLETT|JAMES|M|Westchester|HEALING FACTOR;ADAMANTIUM CLAWS;ENHANCED SENSES|BETA|1882-04-12
STORM|MUNROE|ORORO|F|Cairo|WEATHER CONTROL;FLIGHT|OMEGA|
CYCLOPS|SUMMERS|SCOTT|M|Anchorage|OPTIC BLASTS|ALPHA|
HAVOK|SUMMERS|ALEX|M|Anchorage|ENERGY PROJECTION|ALPHA|
CABLE|SUMMERS|NATHAN|M|Westchester|TELEKINESIS;CYBERNETIC BODY;TIME TRAVEL|ALPHA|
JEAN GREY|GREY|JEAN|F|Annandale-on-Hudson|TELEPATHY;TELEKINESIS|OMEGA|
PROFESSOR X|XAVIER|CHARLES|M|Westchester|TELEPATHY|OMEGA|
BEAST|MCCOY|HANK|M|Dundee|GENIUS INTELLECT;SUPER STRENGTH;SUPER AGILITY|BETA|
ICEMAN|DRAKE|BOBBY|M|Floral Park|CRYOKINESIS|OMEGA|
ROGUE|LEBEAU|ANNA MARIE|F|Caldecott|POWER ABSORPTION;FLIGHT|ALPHA|
GAMBIT|LEBEAU|REMY|M|New Orleans|KINETIC CHARGING|BETA|
NIGHTCRAWLER|WAGNER|KURT|M|Winzeldorf|TELEPORTATION|BETA|
COLOSSUS|RASPUTIN|PIOTR|M|Ust-Ordynsky|INVULNERABILITY;SUPER STRENGTH|ALPHA|
SHADOWCAT|PRYDE|KATHERINE|F|Deerfield|PHASING|BETA|
PSYLOCKE|BRADDOCK|ELIZABETH|F|Maldon|TELEPATHY|ALPHA|
CAPTAIN BRITAIN|BRADDOCK|BRIAN|M|Maldon|SUPER STRENGTH;FLIGHT|ALPHA|
ARCHANGEL|WORTHINGTON III|WARREN|M|Centerport|FLIGHT (WINGS)|BETA|
EMMA FROST|FROST|EMMA|F|Boston|TELEPATHY;INVULNERABILITY|OMEGA|
NORTHSTAR|BEAUBIER|JEAN-PAUL|M|Montreal|SUPER SPEED;FLIGHT|ALPHA|
AURORA|BEAUBIER|JEANNE-MARIE|F|Montreal|SUPER SPEED;FLIGHT|ALPHA|
BANSHEE|CASSIDY|SEAN|M|Cork|SONIC SCREAM;FLIGHT|BETA|
POLARIS|DANE|LORNA|F|Seattle|MAGNETISM|ALPHA|
SUNSPOT|DA COSTA|ROBERTO|M|Rio de Janeiro|SUPER STRENGTH|BETA|
CANNONBALL|GUTHRIE|SAM|M|Cumberland|FLIGHT;INVULNERABILITY|BETA|
DOMINO|THURMAN|NEENA|F|Chicago|PROBABILITY MANIPULATION|BETA|
MULTIPLE MAN|MADROX|JAMIE|M|Topeka|SELF-DUPLICATION|BETA|
DAZZLER|BLAIRE|ALISON|F|Gardendale|ENERGY PROJECTION|BETA|
FIRESTAR|JONES|ANGELICA|F|Richmond|ENERGY PROJECTION;FLIGHT|ALPHA|
MISTER FANTASTIC|RICHARDS|REED|M|Manhattan|ELASTICITY;GENIUS INTELLECT|ALPHA|
INVISIBLE WOMAN|RICHARDS|SUSAN|F|Glenville|INVISIBILITY;FORCE FIELDS|OMEGA|
HUMAN TORCH|STORM|JOHNNY|M|Glenville|PYROKINESIS;FLIGHT|ALPHA|
THE THING|GRIMM|BEN|M|Lower East Side|INVULNERABILITY;SUPER STRENGTH|ALPHA|
SILVER SURFER|RADD|NORRIN|M|Zenn-La|COSMIC POWER;FLIGHT|OMEGA|
NOVA|RIDER|RICHARD|M|Hempstead|FLIGHT;ENERGY PROJECTION|ALPHA|
STAR-LORD|QUILL|PETER|M|St. Charles|GADGETS;MARKSMANSHIP|DELTA|
SHANG-CHI|XU|SHANG-CHI|M|San Francisco|MARTIAL ARTS|BETA|
BLADE|BROOKS|ERIC|M|London|SWORDSMANSHIP;HEALING FACTOR|BETA|
WONDER MAN|WILLIAMS|SIMON|M|Paterson|INVULNERABILITY;SUPER STRENGTH;FLIGHT|OMEGA|
QUASAR|VAUGHN|WENDELL|M|Fond du Lac|ENERGY CONSTRUCTS (QUANTUM BANDS)|OMEGA|
SUNFIRE|YOSHIDA|SHIRO|M|Tokyo|PYROKINESIS;FLIGHT|ALPHA|
TIGRA|NELSON|GREER|F|Chicago|SUPER AGILITY;ENHANCED SENSES|BETA|
HELLCAT|WALKER|PATSY|F|Centerville|MARTIAL ARTS|DELTA|
BLACK BOLT|BOLTAGON|BLACKAGAR|M|Attilan|SONIC BLASTS|OMEGA|
MEDUSA|AMAQUELIN|MEDUSALITH|F|Attilan|PREHENSILE HAIR|BETA|
CLOAK|JOHNSON|TYRONE|M|New Orleans|TELEPORTATION|BETA|
DAGGER|BOWEN|TANDY|F|New Orleans|ENERGY PROJECTION|BETA|
SENTRY|REYNOLDS|ROBERT|M|New York|MATTER MANIPULATION;FLIGHT|OMEGA|
ROCKETEER|SECORD|CLIFF|M|Los Angeles|GADGETS;FLIGHT (JETPACK)|DELTA|
THE PHANTOM|WALKER|KIT|M|Bangalla|STEALTH;MARKSMANSHIP|DELTA|
THE SHADOW|CRANSTON|LAMONT|M|New York|CLOUDING MEN'S MINDS;STEALTH|BETA|
"""


# Signature housing restrictions for the famous pool (everyone else is derived from powers)
FAMOUS_HOUSING = {
    "SUPERMAN": "KRYPTONITE-LINED CELL", "SUPERGIRL": "KRYPTONITE-LINED CELL",
    "POWER GIRL": "KRYPTONITE-LINED CELL", "HULK": "GAMMA-SHIELDED CELL",
    "SHE-HULK": "GAMMA-SHIELDED CELL", "GREEN LANTERN": "YELLOW-PAINTED CELL",
    "WOLVERINE": "WALK-THROUGH METAL DETECTOR EXEMPT", "WONDER WOMAN": "LASSO AND BRACELETS IN PROPERTY",
    "BLACK PANTHER": "VIBRANIUM SUIT IN PROPERTY", "SPIDER-MAN": "ADHESIVE-RESISTANT CELL",
    "THOR": "MJOLNIR IN PROPERTY", "CAPTAIN AMERICA": "SHIELD IN PROPERTY",
    "MARTIAN MANHUNTER": "NO OPEN FLAME", "MISTER MIRACLE": "CONSTANT OBSERVATION",
    "BLACK BOLT": "SOUNDPROOF CELL", "CYCLOPS": "RUBY-QUARTZ VISOR REQUIRED",
    "ROGUE": "NO PHYSICAL CONTACT", "MULTIPLE MAN": "HOURLY HEADCOUNT",
}


def parse_famous():
    out = []
    for line in FAMOUS.strip().splitlines():
        alias, last, first, gender, city, powers, threat, dob = line.split("|")
        out.append({"alias": alias, "last": last, "first": first, "gender": gender, "city": city,
                    "powers": powers.split(";"), "threat": threat,
                    "dob": date.fromisoformat(dob) if dob else None,
                    "housing": FAMOUS_HOUSING.get(alias)})
    return out


# ---------------------------------------------------------------------------------------------
# Procedural vocabulary
# ---------------------------------------------------------------------------------------------
ADJ = """CRIMSON AZURE OBSIDIAN RADIANT SILENT GOLDEN EMERALD COBALT AMBER IVORY SPECTRAL ATOMIC
COSMIC ELECTRIC SOLAR LUNAR MIDNIGHT RAPID MIGHTY ETERNAL FEARLESS PHANTOM NEON QUANTUM SAPPHIRE ONYX
TITANIUM VELVET SCARLET ARCTIC STELLAR SONIC MAGNETIC PRIMAL SAVAGE NOBLE VALIANT GALLANT DAUNTLESS
RESTLESS HOLLOW SHATTERED BURNING FROZEN THUNDERING WHISPERING WANDERING HIDDEN SILVERED GILDED ASHEN
COPPER BRONZE CHROME PLATINUM JADE CERULEAN VERMILION INDIGO VIOLET CRYSTAL NUCLEAR KINETIC ASTRAL
INFINITE ULTRA HYPER OMNI MEGA NIGHT DAWN DUSK STORMBORN IRONCLAD STEADFAST""".split()

NOUN = """SENTINEL TEMPEST WARDEN COMET SPECTER AEGIS VORTEX GUARDIAN TITAN LYNX KESTREL HALCYON BULWARK
PARAGON VANGUARD VOLTAGE MERIDIAN ZENITH ECLIPSE CYCLONE PULSAR MONSOON TORRENT GLACIER INFERNO CINDER
SPARK BEACON BASTION CITADEL RAMPART MIRAGE PRISM COMPASS NOMAD PILGRIM RANGER MARSHAL PALADIN HARBINGER
OMEN WRAITH TALON FANG HOWLER STINGER HORNET CONDOR OSPREY MARLIN BARRACUDA GRIZZLY BISON STALLION
JAGUAR COUGAR MONOLITH OBELISK SPIRE LAMPLIGHTER DYNAMO REACTOR PISTON RIVET ANVIL HAMMERHEAD FULCRUM
AXIOM VECTOR CIPHER ENIGMA RIDDLE SEER SIGNAL STATIC ECHOLOCATOR RIPTIDE UNDERTOW MAELSTROM
SQUALL GALE ZEPHYR SIROCCO MISTRAL BOREALIS AURORAX HELIX PARALLAX QUASARIUS NEBULON STARLING
SWIFT MAGPIE SHRIKE HERON HUSK GOLEM COLOSSUX JUGGERNOT BRAWLER DUELIST FENCER ARCHON REGENT MONARCH
SOVEREIGN TRIBUNE CENTURION LEGIONNAIRE GLADIATOR SAMURAI RONIN VALKYR TEMPLAR CRUSADER""".split()

PREFIX = """VOLT AERO CRYO PYRO TERRA NEURO GRAV CHRONO MAGNA SONI LUMI UMBRA HYDRO STRATO KINE PHOTO
FERRO PHASE NANO QUANTA ASTRO NOVA RADIO THERMO AQUA GEO CYBER PSY BIO TECTO SEISMO FLUX VORTA ZEPHY
HELIO NOCTI SPECTRO TURBO IONO PLASMA""".split()
SUFFIX = """X TRIX NOVA VANCE LYTE TRON STRIKE BLADE FORCE WAVE PULSE SHADE FLUX STORM BOLT SHIFT GUARD
FIRE FROST SURGE SPARK LANCE WING HAWK FIST KNIGHT RIDER WALKER WARD CORE FANG BURST""".split()

TITLES_M = ["CAPTAIN", "DOCTOR", "MISTER", "SERGEANT", "PROFESSOR", "COMMANDER"]
TITLES_F = ["CAPTAIN", "DOCTOR", "MISS", "MADAME", "LADY", "COMMANDER"]
LEGACY = ["II", "III", "2099", "PRIME", "JR"]

LAST_NAMES = """SMITH JOHNSON WILLIAMS BROWN JONES GARCIA MILLER DAVIS RODRIGUEZ MARTINEZ HERNANDEZ LOPEZ
GONZALEZ WILSON ANDERSON THOMAS TAYLOR MOORE JACKSON MARTIN LEE PEREZ THOMPSON WHITE HARRIS SANCHEZ
CLARK RAMIREZ LEWIS ROBINSON WALKER YOUNG ALLEN KING WRIGHT SCOTT TORRES NGUYEN HILL FLORES GREEN
ADAMS NELSON BAKER HALL RIVERA CAMPBELL MITCHELL CARTER ROBERTS GOMEZ PHILLIPS EVANS TURNER DIAZ
PARKER CRUZ EDWARDS COLLINS REYES STEWART MORRIS MORALES MURPHY COOK ROGERS GUTIERREZ ORTIZ MORGAN
COOPER PETERSON BAILEY REED KELLY HOWARD RAMOS KIM COX WARD RICHARDSON WATSON BROOKS CHAVEZ WOOD
JAMES BENNETT GRAY MENDOZA RUIZ HUGHES PRICE ALVAREZ CASTILLO SANDERS PATEL MYERS LONG ROSS FOSTER
JIMENEZ POWELL JENKINS PERRY RUSSELL SULLIVAN BELL COLEMAN BUTLER HENDERSON BARNES GONZALES FISHER
VASQUEZ SIMMONS ROMERO JORDAN PATTERSON ALEXANDER HAMILTON GRAHAM REYNOLDS GRIFFIN WALLACE MORENO
WEST COLE HAYES BRYANT HERRERA GIBSON ELLIS TRAN MEDINA AGUILAR STEVENS MURRAY FORD CASTRO MARSHALL
OWENS HARRISON FERNANDEZ MCDONALD WOODS WASHINGTON KENNEDY WELLS VARGAS HENRY CHEN FREEMAN WEBB
TUCKER GUZMAN BURNS CRAWFORD OLSON SIMPSON PORTER HUNTER GORDON MENDEZ SILVA SHAW SNYDER MASON DIXON
MUNOZ HUNT HICKS HOLMES PALMER WAGNER BLACK ROBERTSON BOYD ROSE STONE SALAZAR FOX WARREN MILLS
MEYER RICE SCHMIDT GARZA DANIELS FERGUSON NICHOLS STEPHENS SOTO WEAVER RYAN GARDNER PAYNE GRANT
DUNN KELLEY SPENCER HAWKINS ARNOLD PIERCE VAZQUEZ HANSEN PETERS SANTOS HART BRADLEY KNIGHT ELLIOTT
OKAFOR NWOSU BECKER DOMINGUEZ FUENTES ESPINOZA CERVANTES OCHOA VILLARREAL LUNA TREVINO""".split()
# Awkward surnames that break naive parsing and title-casing
LAST_NAMES += ["O'CONNOR", "O'NEAL", "MCBRIDE", "MACDONALD", "DE LA CRUZ", "VAN BUREN",
               "ST. JAMES", "GARCIA-LOPEZ", "DEL RIO", "SMITH JR", "JOHNSON III"]

MALE_FIRST = """JAMES MICHAEL ROBERT JOHN DAVID WILLIAM RICHARD JOSEPH THOMAS CHRISTOPHER CHARLES DANIEL
MATTHEW ANTHONY MARK DONALD STEVEN ANDREW PAUL JOSHUA KENNETH KEVIN BRIAN TIMOTHY RONALD GEORGE
JASON EDWARD JEFFREY RYAN JACOB NICHOLAS GARY ERIC JONATHAN STEPHEN LARRY JUSTIN SCOTT BRANDON
BENJAMIN SAMUEL GREGORY ALEXANDER PATRICK FRANK RAYMOND JACK DENNIS JERRY TYLER AARON JOSE ADAM
NATHAN HENRY ZACHARY DOUGLAS PETER KYLE NOAH ETHAN JEREMY CHRISTIAN WALTER KEITH AUSTIN ROGER TERRY
SEAN GERALD CARL DYLAN HAROLD JORDAN JESSE BRYAN LAWRENCE ARTHUR GABRIEL BRUCE LOGAN BILLY JOE
ALAN JUAN ELIJAH WILLIE ALBERT WAYNE RANDY MASON VINCENT LIAM ROY BOBBY CALEB BRADLEY RUSSELL LUCAS
CARLOS LUIS MIGUEL JESUS ANTONIO MARCUS TERRENCE DEMETRIUS CODY TRAVIS DUSTIN COREY DERRICK RAVI""".split()
MALE_FIRST += ["JEAN-LUC", "JOHN PAUL"]

FEMALE_FIRST = """MARY PATRICIA JENNIFER LINDA ELIZABETH BARBARA SUSAN JESSICA SARAH KAREN LISA NANCY BETTY
SANDRA MARGARET ASHLEY KIMBERLY EMILY DONNA MICHELLE CAROL AMANDA MELISSA DEBORAH STEPHANIE REBECCA
SHARON LAURA CYNTHIA AMY KATHLEEN ANGELA SHIRLEY BRENDA EMMA ANNA PAMELA NICOLE SAMANTHA KATHERINE
CHRISTINE HELEN DEBRA RACHEL CAROLYN JANET MARIA HEATHER DIANE JULIE JOYCE VICTORIA KELLY CHRISTINA
LAUREN JOAN EVELYN OLIVIA JUDITH MEGAN CHERYL ANDREA HANNAH MARTHA JACQUELINE FRANCES GLORIA TERESA
KAYLA ROSA CRYSTAL TIFFANY BRITTANY ERICA TAMMY DESTINY VERONICA YOLANDA ALICIA MONICA""".split()
FEMALE_FIRST += ["MARY-KATE", "ANNA MARIE"]

CITIES = """Metropolis|Gotham City|Central City|Star City|Coast City|Keystone City|Bludhaven|National City|
Opal City|Hub City|Midway City|Ivy Town|Fawcett City|Jump City|Westview|Calvin City|Freeland|Nueva York|
New York|Los Angeles|Chicago|Houston|Dallas|San Antonio|El Paso|Austin|Detroit|Boston|Seattle|Miami|
Atlanta|New Orleans|Denver|Phoenix|Philadelphia|Baltimore|Portland|San Francisco|Pittsburgh|Cleveland|
London|Tokyo|Cairo|Montreal|Rio de Janeiro|Mexico City|Lagos|Mumbai|Sydney|Hell's Kitchen|K'un-Lun|
Winston-Salem|Wilkes-Barre|St. Louis|Fond du Lac""".replace("\n", "").split("|")

# power -> tier; threat level = highest tier among a persona's powers
POWERS = {
    "DELTA": ["MARTIAL ARTS", "GADGETS", "GENIUS INTELLECT", "MARKSMANSHIP", "ACROBATICS", "HACKING",
              "DETECTIVE SKILLS", "STEALTH", "ESPIONAGE", "SWORDSMANSHIP", "MASTER ARCHERY",
              "UNLIMITED BUDGET", "PARKOUR", "DEMOLITIONS"],
    "BETA": ["ENHANCED STRENGTH", "WALL-CRAWLING", "HEALING FACTOR", "ENHANCED SENSES",
             "SIZE MANIPULATION", "ELASTICITY", "INVISIBILITY", "PHASING", "SONIC SCREAM",
             "ANIMAL MIMICRY", "CAMOUFLAGE", "GLIDING", "SUPER AGILITY", "NIGHT VISION",
             "UNDERWATER BREATHING", "SELF-DUPLICATION", "BIOLUMINESCENCE", "KINETIC CHARGING"],
    "ALPHA": ["FLIGHT", "SUPER STRENGTH", "SUPER SPEED", "TELEPORTATION", "TELEKINESIS",
              "PYROKINESIS", "CRYOKINESIS", "ELECTRICITY MANIPULATION", "ENERGY PROJECTION",
              "FORCE FIELDS", "SHAPESHIFTING", "TECHNOPATHY", "HYDROKINESIS", "MAGNETISM",
              "DENSITY CONTROL", "SONIC BLASTS", "POWERED ARMOR", "INVULNERABILITY", "TELEPATHY"],
    "OMEGA": ["WEATHER CONTROL", "REALITY WARPING", "TIME MANIPULATION", "MATTER MANIPULATION",
              "GRAVITY MANIPULATION", "COSMIC POWER", "CHAOS MAGIC", "MOLECULAR MANIPULATION"],
}
TIER_ORDER = ["DELTA", "BETA", "ALPHA", "OMEGA"]
THREAT_WEIGHTS = [30, 42, 22, 6]

HOUSING = {
    "WEATHER CONTROL": "INTERIOR CELL NO SKY ACCESS", "REALITY WARPING": "NULLIFIER FIELD",
    "CHAOS MAGIC": "NULLIFIER FIELD", "COSMIC POWER": "NULLIFIER FIELD", "SORCERY": "NULLIFIER FIELD",
    "TIME MANIPULATION": "CHRONAL ANCHOR", "TIME TRAVEL": "CHRONAL ANCHOR",
    "GRAVITY MANIPULATION": "NULLIFIER FIELD", "MATTER MANIPULATION": "NULLIFIER FIELD",
    "MOLECULAR MANIPULATION": "NULLIFIER FIELD", "TELEPATHY": "PSIONIC DAMPENER",
    "TELEKINESIS": "PSIONIC DAMPENER", "TELEPORTATION": "TELEPORT INHIBITOR COLLAR",
    "SUPER SPEED": "SPEED-DAMPENING COLLAR", "SUPER STRENGTH": "REINFORCED CELL",
    "PYROKINESIS": "FIRE-SUPPRESSION CELL", "CRYOKINESIS": "HEATED CELL",
    "ELECTRICITY MANIPULATION": "GROUNDED CELL", "ENERGY PROJECTION": "ENERGY-ABSORBENT CELL",
    "SONIC BLASTS": "SOUNDPROOF CELL", "SONIC SCREAM": "SOUNDPROOF CELL",
    "FORCE FIELDS": "ENERGY-ABSORBENT CELL", "SHAPESHIFTING": "DAILY BIOMETRIC ID CHECK",
    "TECHNOPATHY": "NO ELECTRONICS", "HYDROKINESIS": "WATERLESS CELL", "MAGNETISM": "NON-METALLIC CELL",
    "PHASING": "PHASE-PROOF CELL", "INVISIBILITY": "THERMAL MONITORING",
    "SIZE MANIPULATION": "MICRO-MESH VENTS", "ELASTICITY": "SEALED VENTS AND DRAINS",
    "WALL-CRAWLING": "SMOOTH-WALL CELL", "FLIGHT": "NO OUTDOOR RECREATION",
    "POWERED ARMOR": "ARMOR IN PROPERTY", "GADGETS": "UTILITY BELT IN PROPERTY",
    "SELF-DUPLICATION": "HOURLY HEADCOUNT", "ESCAPE ARTISTRY": "CONSTANT OBSERVATION",
    "POWER ABSORPTION": "NO PHYSICAL CONTACT", "OPTIC BLASTS": "RUBY-QUARTZ VISOR REQUIRED",
    "INVULNERABILITY": "REINFORCED CELL", "DENSITY CONTROL": "PHASE-PROOF CELL",
    "ENHANCED STRENGTH": "REINFORCED CELL", "HACKING": "NO ELECTRONICS", "STEALTH": "CONSTANT OBSERVATION",
    "ESPIONAGE": "CONSTANT OBSERVATION", "SWORDSMANSHIP": "NO TOOL ACCESS", "ARCHERY": "NO TOOL ACCESS",
    "MARKSMANSHIP": "NO TOOL ACCESS", "GENIUS INTELLECT": "NO TOOL ACCESS", "ACROBATICS": "LOW-CEILING CELL",
    "PARKOUR": "LOW-CEILING CELL", "SUPER AGILITY": "LOW-CEILING CELL", "DEMOLITIONS": "NO CHEMICAL ACCESS",
    "UNLIMITED BUDGET": "COMMISSARY LIMIT ENFORCED", "MARTIAL ARTS": "SINGLE-CELL STATUS",
    "ENHANCED SENSES": "QUIET CELL BLOCK", "ANIMAL MIMICRY": "NO ANIMAL CONTACT",
    "CAMOUFLAGE": "THERMAL MONITORING", "GLIDING": "NO OUTDOOR RECREATION",
    "NIGHT VISION": "LIGHTS ON 24/7", "KINETIC CHARGING": "NO LOOSE OBJECTS",
}

# offense, violence code, (min, max) years, weight, required power (or None)
OFFENSES = [
    ("UNAUTH FLIGHT IN RESTRICTED AIRSPACE", "NV", (1, 5), 6, "FLIGHT"),
    ("SPEEDING >=500 MPH", "NV", (1, 4), 3, "SUPER SPEED"),
    ("UNLICENSED USE OF SUPERPOWERS", "NV", (1, 5), 10, None),
    ("UNL POSS PROHIBITED WEAPON (ALIEN TECH)", "NV", (2, 8), 4, None),
    ("TRESPASS ON SECURE FACILITY", "NV", (1, 5), 6, None),
    ("IMPERSONATION OF PUBLIC SERVANT", "NV", (2, 6), 4, None),
    ("BREACH OF COMPUTER SECURITY", "NV", (2, 10), 4, None),
    ("TAMPER W/EVID (TIME TRAVEL)", "NV", (2, 10), 1, "TIME TRAVEL"),
    ("UNAUTH INTERDIMENSIONAL TRAVEL", "NV", (2, 8), 2, None),
    ("EVADING ARREST (FLIGHT)", "NV", (1, 5), 3, "FLIGHT"),
    ("OBSTRUCT HWY W/WEBBING", "NV", (1, 3), 1, "WALL-CRAWLING"),
    ("CRIM MISCHIEF >=$150K<$300K", "V1", (2, 10), 7, None),
    ("CRIM MISCHIEF >=$300K", "V1", (5, 20), 6, None),
    ("CRIM MISCHIEF >=$300K, HABITUAL", "V1", (10, 40), 3, None),
    ("ARSON (PYROKINETIC)", "V1", (2, 20), 3, "PYROKINESIS"),
    ("RECKLESS DAMAGE BY ENERGY BLAST", "V1", (2, 15), 4, "ENERGY PROJECTION"),
    ("DESTRUCTION OF INFRASTRUCTURE (BRIDGE)", "V1", (5, 25), 3, None),
    ("BURGLARY OF VILLAIN LAIR", "V1", (2, 10), 5, None),
    ("CRIM MISCHIEF BY WEATHER EVENT >=$150K<$300K", "V1", (2, 15), 1, "WEATHER CONTROL"),
    ("VIGILANTISM W/GRAPPLING DEVICE", "V2", (2, 10), 5, "GADGETS"),
    ("ASSAULT ON HENCHMAN", "V2", (2, 10), 8, None),
    ("UNLAWFUL RESTRAINT OF SUSPECT", "V2", (2, 10), 6, None),
    ("COERCED CONFESSION W/TELEPATHY", "V2", (2, 10), 2, "TELEPATHY"),
    ("ASSAULT PUB SERV (MISTAKEN IDENTITY)", "V2", (2, 10), 4, None),
    ("AGG ASSAULT W/SUPERPOWER", "V3", (5, 30), 7, None),
    ("AGG ASSAULT W/DEADLY WEAPON", "V3", (5, 25), 4, None),
    ("AGG ASSAULT W/ADAMANTIUM CLAWS", "V3", (5, 30), 1, "ADAMANTIUM CLAWS"),
    ("MANSLAUGHTER (COLLATERAL DAMAGE)", "V3", (5, 20), 2, None),
]
CUSTODY_BY_VIOLENCE = {
    "NV": [30, 45, 20, 4, 1], "V1": [15, 40, 30, 10, 5],
    "V2": [5, 25, 45, 18, 7], "V3": [1, 10, 34, 35, 20],
}
CUSTODY = ["G1", "G2", "G3", "G4", "G5"]


# ---------------------------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------------------------
def fmt_date(d: date) -> str:
    """9 string formats. Built by hand so years < 1000 stay 4 digits on every platform."""
    Y, m, dd = f"{d.year:04d}", d.month, d.day
    mon, month = calendar.month_abbr[m], calendar.month_name[m]
    fmts = [
        (f"{m:02d}/{dd:02d}/{Y}", 22), (f"{m}/{dd}/{d.year % 100:02d}", 10),
        (f"{m:02d}-{dd:02d}-{Y}", 8), (f"{Y}-{m:02d}-{dd:02d}", 18),
        (f"{Y}-{m:02d}-{dd:02d} 00:00:00", 10), (f"{Y}{m:02d}{dd:02d}", 12),
        (f"{dd:02d}-{mon.upper()}-{Y}", 10), (f"{month} {dd}, {Y}", 5), (f"{mon} {dd} {Y}", 5),
    ]
    return rng.choices([f[0] for f in fmts], [f[1] for f in fmts])[0]


def mix_case(name: str) -> str:
    if not name.strip():
        return name
    style = case_rng.choices(["upper", "title", "lower", "random"], [40, 30, 15, 15])[0]
    if style == "upper":
        return name.upper()
    if style == "title":
        return name.title()
    if style == "lower":
        return name.lower()
    letters = sum(c.isalpha() for c in name)
    while True:
        out = "".join(c.upper() if case_rng.random() < 0.5 else c.lower() for c in name)
        if not (out.isupper() or out.islower()) or letters < 2:
            return out


def padded_null() -> str:
    pad = " " * rng.randint(1, 8)
    return pad if rng.random() < 0.85 else pad[: len(pad) // 2] + "\t" + pad[len(pad) // 2:]


# ---------------------------------------------------------------------------------------------
# Persona generation
# ---------------------------------------------------------------------------------------------
def procedural_alias(gender: str, blocked: set) -> str:
    """Weighted toward large combination spaces so most aliases are unique, some repeat."""
    titles = TITLES_F if gender == "F" else TITLES_M
    while True:
        coined = rng.choice(PREFIX) + rng.choice(SUFFIX)
        pattern = rng.choices(range(7), [45, 25, 8, 10, 4, 5, 3])[0]
        alias = [
            lambda: f"{rng.choice(ADJ)} {coined}",                  # ~100k combos
            lambda: f"{rng.choice(ADJ)} {rng.choice(NOUN)}",        # ~9k combos
            lambda: f"THE {rng.choice(ADJ)} {rng.choice(NOUN)}",    # near-duplicate of the above
            lambda: f"{rng.choice(titles)} {coined}",               # ~8k combos
            lambda: f"{rng.choice(titles)} {rng.choice(NOUN)}",     # small space, repeats often
            lambda: coined,                                          # ~1.3k combos, repeats often
            lambda: f"{rng.choice(NOUN)}-{'WOMAN' if gender == 'F' else 'MAN'}",
        ][pattern]()
        if rng.random() < 0.06:
            alias += " " + rng.choice(LEGACY)
        if alias not in blocked:
            return alias


def procedural_powers():
    threat = rng.choices(TIER_ORDER, THREAT_WEIGHTS)[0]
    top = TIER_ORDER.index(threat)
    powers = [rng.choice(POWERS[threat])]
    for _ in range(rng.choices([0, 1, 2], [45, 40, 15])[0]):
        extra = rng.choice(POWERS[TIER_ORDER[rng.randint(0, top)]])
        if extra not in powers:
            powers.append(extra)
    return powers, threat


def housing_for(powers: list) -> str:
    for p in powers:  # first power listed is the primary one
        for key, restriction in HOUSING.items():
            if key in p:
                return restriction
    return "NONE"  # semantic null as text: a common real-world trap


def pick_offense(powers: list):
    joined = ";".join(powers)
    eligible = [o for o in OFFENSES if o[4] is None or o[4] in joined]
    return rng.choices(eligible, [o[3] for o in eligible])[0]


def build_records(persona: dict, n_records: int) -> list:
    """One persona -> 1..n incarcerations. Earlier ones are released; the latest is current."""
    sentences = sorted(TODAY - timedelta(days=int(rng.expovariate(1 / 1500)) % (365 * 30))
                       for _ in range(n_records))
    if persona["dob"] is None:
        age = rng.randint(18, 60)
        persona["dob"] = date(sentences[0].year - age, rng.randint(1, 12), rng.randint(1, 28))
    records = []
    for i, sentence in enumerate(sentences):
        offense, vcode, (lo, hi), _, _ = pick_offense(persona["powers"])
        release = sentence + timedelta(days=int(rng.uniform(lo, hi) * 365.25))
        if i < len(sentences) - 1:  # prior incarceration: released before the next sentence
            nxt = sentences[i + 1]
            release = min(release, nxt - timedelta(days=rng.randint(30, 400)))
            if release <= sentence:
                release = sentence + timedelta(days=rng.randint(60, 300))
        elif release <= TODAY:
            release = TODAY + timedelta(days=rng.randint(30, 1500))
        records.append({
            "sid": persona["sid"], "last": persona["last"], "first": persona["first"],
            "alias": persona["alias"], "dob": persona["dob"], "gender": persona["gender"],
            "city": persona["city"],
            "custody": rng.choices(CUSTODY, CUSTODY_BY_VIOLENCE[vcode])[0],
            "vcode": vcode, "threat": persona["threat"], "powers": ";".join(persona["powers"]),
            "offense": offense, "housing": persona.get("housing") or housing_for(persona["powers"]),
            "sentence": sentence, "release": release,
        })
    return records


# ---------------------------------------------------------------------------------------------
# v2 columns. Everything here uses v2_rng only, so v1 columns stay byte-identical.
# ---------------------------------------------------------------------------------------------
NUM_WORDS = ["zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"]
PROPERTY_OFFENSE_WORDS = ("MISCHIEF", "ARSON", "DESTRUCTION", "BURGLARY", "RECKLESS DAMAGE")
ESCAPE_POWER_WORDS = ("SPEED", "TELEPORT", "PHASING", "FLIGHT", "SHAPESHIFT", "ESCAPE", "SIZE")
ESCAPE_BASE = {"G1": 0.02, "G2": 0.05, "G3": 0.12, "G4": 0.30, "G5": 0.50}
TRUE_FORMS = (["Y", "Yes", "YES", "y", "1", "TRUE", "T", "X"], [40, 15, 10, 5, 10, 8, 5, 7])
FALSE_FORMS = (["N", "No", "NO", "n", "0", "FALSE", "F"], [42, 16, 11, 5, 11, 9, 6])

OFFICERS = ["Ramirez", "Lee", "Goode", "Goodman", "Nguyen", "Patel", "Brooks", "Davis", "Okafor", "Castillo"]
NOTE_PREFIXES = ["", "", "", "Day shift: ", "2nd shift - ", "3rd shift: ", "C/O {off} reports ",
                 "Per Sgt. {off}, ", "Obs: ", "Count note: "]
NOTE_SUFFIXES = ["", "", "", "", " Will continue to monitor.", " No use of powers observed.",
                 " Cape stored in property.", " Logged by C/O {off}."]

POSITIVE = [
    "{S} was good during count", "{S} had a good attitude today", "good behavior in the dayroom",
    "{S} was GOOD all shift", "goood attitude, no complaints", "{S} was gud today",
    "behavior was better than last week", "best behavior on the block this week",
    "{S} was behaving well at chow", "{S} did well in the workshop", "{S} was great during rec",
    "excellent conduct during the fire drill", "outstanding work detail performance",
    "{S} was cooperative and respectful", "{S} was polite and courteous with staff",
    "{S} was compliant with all orders", "no issues this shift", "no problems reported",
    "no incidents, followed every instruction", "zero write-ups this month", "{S} is squared away",
    "model inmate, keeps area clean", "a pleasure to supervise", "{S} gave us no trouble",
    "{S} did everything asked without complaint", "solid week, stayed out of trouble",
    "exemplary conduct, recommend for trusty status", "{S} helped staff during the kitchen fire",
    "{S} used powers only when authorized", "{S} volunteered to fix the rec yard lights",
    "positive attitude, doing fine", "{S} was fine, quiet and respectful", "10/10 behavior",
    "A+ attitude in class", "commendable effort in GED class", "{S} de-escalated a dayroom argument",
    "stellar week",
]
NEGATIVE = [
    "{S} was not good during count", "attitude was no good today", "{S}'s behavior wasn't good at chow",
    "behavior far from good this week", "{S} was anything but good during rec", "not a good day for {O}",
    "{S} was disruptive in the dayroom", "{S} was combative with staff", "{S} was hostile and uncooperative",
    "{S} was not cooperative during search", "{S} was disrespectful to the officer",
    "{S} refused orders to return to cell", "caused a disturbance at chow",
    "{S} attempted to fly over the perimeter fence", "{S} phased through the cell wall during count",
    "{S} refused to remove cape for search", "{S} threatened staff with heat vision",
    "{S} started a fight in the dayroom", "written up twice this week", "bad attitude, argued with staff",
    "terrible behavior during transport", "poor conduct at work detail", "{S} was belligerent and aggressive",
    "contraband goods confiscated from cell", "{S} is no longer compliant with orders",
    "not exactly a model inmate this week", "{S} teleported out of restraints again",
    "{S} was insubordinate during count",
]
NEUTRAL = [
    "nothing to report", "routine count, no change", "{S} attended orientation",
    "visitation scheduled for Saturday", "moved to cell block C", "good time credit applied per policy",
    "good conduct time recalculated by classification", "commissary order processed",
    "awaiting classification review", "{S} transferred to medical for scheduled checkup",
    "property inventory completed", "cape logged into personal property",
    "{S} requested law library access", "no contact this shift", "mail received and inspected",
    "{S} attended scheduled education class", "goods received from approved vendor",
    "paperwork sent to Mr. Goodman in records",
]
MIXED = [
    "good in the morning but combative by evening", "{S} was cooperative at count but refused work detail",
    "started the week well, ended with a write-up", "respectful to staff, fought with cellmate",
]
LABEL_TEMPLATES = {"positive": POSITIVE, "negative": NEGATIVE, "neutral": NEUTRAL, "mixed": MIXED}
LABEL_WEIGHTS = {  # by violence code: violent offenders skew negative
    "NV": [48, 22, 25, 5], "V1": [42, 28, 25, 5], "V2": [35, 35, 25, 5], "V3": [28, 42, 25, 5],
}


def _typo(text: str) -> str:
    words = text.split(" ")
    idx = [i for i, w in enumerate(words) if len(w) >= 4 and w.isalpha()]
    if not idx:
        return text
    i = v2_rng.choice(idx)
    w = words[i]
    j = v2_rng.randint(1, len(w) - 2)
    words[i] = w[:j] + w[j + 1:] if v2_rng.random() < 0.5 else w[:j] + w[j] + w[j:]
    return " ".join(words)


def make_note(vcode: str, gender: str, alias: str):
    """Return (note_text, true_label)."""
    weights = LABEL_WEIGHTS.get(vcode, LABEL_WEIGHTS["V1"])
    label = v2_rng.choices(["positive", "negative", "neutral", "mixed"], weights)[0]
    core = v2_rng.choice(LABEL_TEMPLATES[label])
    pronoun = "She" if gender == "F" else "He"
    subject = v2_rng.choice(["Inmate", "Offender", "I/M", "Subject", pronoun, pronoun,
                             (alias or "Inmate").title()])
    obj = {"She": "her", "He": "him"}.get(subject, "the inmate")
    off = v2_rng.choice(OFFICERS)
    note = core.replace("{S}", subject).replace("{O}", obj)
    note = v2_rng.choice(NOTE_PREFIXES).replace("{off}", off) + note
    note = note[0].upper() + note[1:] + "." + v2_rng.choice(NOTE_SUFFIXES).replace("{off}", off)
    if v2_rng.random() < 0.03:
        note = _typo(note)
    style = v2_rng.random()
    if style < 0.10:
        note = note.upper()
    elif style < 0.15:
        note = note.lower()
    return note, label


def fmt_points(points: int) -> str:
    if v2_rng.random() < 0.002:
        return v2_rng.choice(["2.5", "see file", "TBD"])  # unreadable: must be reported, not guessed
    style = v2_rng.choices(["plain", "zero", "float", "pts", "word", "na"], [60, 10, 10, 7, 10, 3])[0]
    if style == "zero":
        return f"{points:02d}"
    if style == "float":
        return f"{points}.0"
    if style == "pts":
        return f"{points} pts"
    if style == "word" and points <= 10:
        return NUM_WORDS[points]
    if style == "na":
        return "N/A"
    return str(points)


def fmt_money(amount: float) -> str:
    if v2_rng.random() < 0.002:
        return v2_rng.choice(["TBD", "pending", "$1,2O0.00"])  # the last one has a letter O, not a zero
    if amount == 0:
        return v2_rng.choice(["0", "0.00", "$0.00", "-", "-"])
    if amount < 0:
        return f"({abs(amount):,.2f})"
    style = v2_rng.choices(["dollar", "plain", "comma", "dollar_space", "usd", "short"], [35, 20, 10, 5, 5, 25])[0]
    if style == "dollar":
        return f"${amount:,.2f}"
    if style == "plain":
        return f"{amount:.2f}"
    if style == "comma":
        return f"{amount:,.2f}"
    if style == "dollar_space":
        return f"$ {amount:,.2f}"
    if style == "usd":
        return f"USD {amount:,.2f}"
    return f"{round(amount, 2)}"  # "1234.5" style: drops trailing zero


def fmt_time(h: int, m: int) -> str:
    if v2_rng.random() < 0.02:
        return "99:99"  # legacy "unknown" sentinel
    if v2_rng.random() < 0.002:
        return v2_rng.choice(["25:10", "12:75", "noon"])  # right shape or plain words, not a real time
    h12, ampm = (h % 12 or 12), ("AM" if h < 12 else "PM")
    return v2_rng.choices([
        f"{h:02d}:{m:02d}", f"{h12}:{m:02d} {ampm}", f"{h12:02d}:{m:02d} {ampm.lower()}",
        f"{h:02d}{m:02d}", f"{h:02d}:{m:02d}:00", f"{h12}:{m:02d}{ampm}", f"{h:02d}.{m:02d}",
    ], [30, 20, 10, 15, 15, 5, 5])[0]


def v2_fields(custody: str, vcode: str, powers: str, offense: str, gender: str, alias: str):
    """Values for the v2 columns plus the answer-key label, before any nulls are injected."""
    note, label = make_note(vcode, gender, alias)
    lo, hi = {"positive": (0, 3), "neutral": (0, 8), "mixed": (3, 12), "negative": (5, 40)}[label]
    points = v2_rng.randint(lo, hi)

    if any(w in offense for w in PROPERTY_OFFENSE_WORDS):
        amount = round(min(v2_rng.lognormvariate(9.5, 1.6), 2_500_000), 2)
    elif v2_rng.random() < 0.30:
        amount = round(v2_rng.uniform(50, 5000), 2)
    else:
        amount = 0.0
    if v2_rng.random() < 0.01:
        amount = -round(v2_rng.uniform(5, 200), 2)  # overpayment credit

    p = ESCAPE_BASE.get(custody, 0.1) + (0.15 if any(w in powers for w in ESCAPE_POWER_WORDS) else 0)
    if v2_rng.random() < 0.01:
        escape = v2_rng.choice(["U", "UNK"])
    elif v2_rng.random() < 0.002:
        escape = v2_rng.choice(["MAYBE", "?", "PENDING"])
    elif v2_rng.random() < p:
        escape = v2_rng.choices(*TRUE_FORMS)[0]
    else:
        escape = v2_rng.choices(*FALSE_FORMS)[0]

    hour = v2_rng.choices(range(24), [1] * 6 + [4] * 12 + [2] * 6)[0]
    return [fmt_time(hour, v2_rng.randint(0, 59)), fmt_points(points), fmt_money(amount), escape, note], label


def v2_nulls(values: list) -> list:
    out = []
    for v in values:
        roll = v2_rng.random()
        if roll < TRUE_NULL_RATE:
            out.append("")
        elif roll < TRUE_NULL_RATE + PADDED_NULL_RATE:
            pad = " " * v2_rng.randint(1, 8)
            out.append(pad if v2_rng.random() < 0.85 else pad[: len(pad) // 2] + "\t" + pad[len(pad) // 2:])
        else:
            out.append(v)
    return out


# ---------------------------------------------------------------------------------------------
# Single-character flags. Everything here uses v3_rng only, so all earlier columns stay identical.
# ---------------------------------------------------------------------------------------------
DAMPENER_POWER_WORDS = ("HEAT VISION", "OPTIC", "TELEKINESIS", "TELEPATHY", "TELEPORT", "PHASING", "PYROKINESIS",
                        "CRYOKINESIS", "HYDROKINESIS", "ELECTRICITY", "ENERGY", "SONIC", "MAGNETISM", "REALITY",
                        "CHAOS", "SORCERY", "MAGIC", "COSMIC", "GRAVITY", "MATTER", "MOLECULAR", "TIME",
                        "WEATHER", "TECHNOPATHY", "DENSITY", "PROBABILITY", "MINDS")


def flag_fields(threat: str, powers: str) -> list:
    """protective_custody and dampener_required as a TDCJ-style source system would export them."""
    # protective_custody: ~22% of records came from the newer system, which writes T/F instead of Y/N
    if v3_rng.random() < 0.004:
        pc = "U"
    else:
        yes, no = ("T", "F") if v3_rng.random() < 0.22 else ("Y", "N")
        pc = yes if v3_rng.random() < 0.07 else no
        if v3_rng.random() < 0.05:
            pc = pc.lower()
    # dampener_required: T/F only; required for high threat levels and dangerous powers
    if v3_rng.random() < 0.003:
        damp = "?"
    else:
        risky = threat in ("ALPHA", "OMEGA") or any(w in powers for w in DAMPENER_POWER_WORDS)
        damp = "T" if v3_rng.random() < (0.85 if risky else 0.05) else "F"
        if v3_rng.random() < 0.05:
            damp = damp.lower()
    return [pc, damp]


def v3_nulls(values: list) -> list:
    out = []
    for v in values:
        roll = v3_rng.random()
        if roll < TRUE_NULL_RATE:
            out.append("")
        elif roll < TRUE_NULL_RATE + PADDED_NULL_RATE:
            out.append(" " * v3_rng.randint(1, 8))
        else:
            out.append(v)
    return out


# ---------------------------------------------------------------------------------------------
# v4 coded domains and text damage. Everything here uses v4_rng only, so every earlier value is unchanged.
# Race, religion and STG are drawn per person at random, independently of everything else.
# ---------------------------------------------------------------------------------------------
# concept: (weight per person, [(spelling as a source system writes it, weight), ...])
RACE_CONCEPTS = {
    "W": (29, [("W", 50), ("WHITE", 15), ("White", 10), ("w", 5), ("Caucasian", 8), ("CAUC", 4), ("WHT", 4),
               ("W - WHITE", 4)]),
    "B": (32, [("B", 50), ("BLACK", 15), ("Black", 8), ("b", 5), ("African American", 10), ("AFR AMER", 4),
               ("BLK", 5), ("B - BLACK", 3)]),
    "H": (31, [("H", 50), ("HISPANIC", 15), ("Hispanic", 8), ("h", 4), ("Latino", 6), ("LATINO/A", 3), ("HISP", 6),
               ("Mexican American", 4), ("H - HISPANIC", 4)]),
    "A": (1.0, [("A", 50), ("ASIAN", 25), ("Asian", 15), ("a", 10)]),
    "API": (0.4, [("Asian/Pacific Islander", 50), ("API", 30), ("A/PI", 20)]),
    "PI": (0.2, [("Pacific Islander", 50), ("Native Hawaiian", 30), ("PAC ISL", 20)]),
    "I": (0.5, [("I", 45), ("AMERICAN INDIAN", 20), ("Native American", 20), ("AMER IND", 10), ("Alaska Native", 5)]),
    "MENA": (0.5, [("Middle Eastern", 40), ("Arab", 20), ("MENA", 20), ("North African", 20)]),
    "O": (0.8, [("O", 60), ("OTHER", 30), ("Other", 10)]),
    "MULTI": (0.5, [("Multiracial", 40), ("TWO OR MORE", 30), ("Multi", 20), ("2+", 10)]),
    "WH": (1.2, [("W/H", 60), ("WHITE HISPANIC", 25), ("White-Hispanic", 15)]),
    "BH": (0.3, [("B/H", 60), ("BLACK HISPANIC", 40)]),
    "U": (1.2, [("U", 40), ("UNK", 25), ("UNKNOWN", 20), ("?", 15)]),
    "D": (0.6, [("DECLINED", 50), ("Refused", 25), ("Declined to state", 25)]),
}
RACE_JUNK = ["SEE BOOKING", "XX", "Z"]

RELIGION_GROUPS = {
    "Catholic": (22, ["CATHOLIC", "Catholic", "Roman Catholic", "RC", "R.C.", "Cath", "CATHLIC"]),
    "Baptist": (18, ["BAPTIST", "Baptist", "Southern Baptist", "SBC", "Baptis"]),
    "Non-denominational Christian": (12, ["CHRISTIAN", "Christian", "NON-DENOMINATIONAL", "Non-Denom", "NONDENOM",
                                          "Christian - Non-Denom"]),
    "Protestant": (5, ["PROTESTANT", "Protestant", "Prot"]),
    "Methodist": (3, ["METHODIST", "United Methodist", "UMC"]),
    "Pentecostal": (4, ["PENTECOSTAL", "Assembly of God", "AOG"]),
    "Church of Christ": (3, ["CHURCH OF CHRIST", "COC"]),
    "Jehovah's Witness": (2, ["JEHOVAH'S WITNESS", "Jehovah Witness", "JW"]),
    "Latter-day Saint": (1, ["LDS", "Mormon", "LATTER-DAY SAINT"]),
    "Orthodox Christian": (0.5, ["ORTHODOX", "Greek Orthodox", "Eastern Orthodox"]),
    "Muslim": (5, ["MUSLIM", "Muslim", "Islam", "ISLAMIC", "Muslim (Sunni)", "Sunni", "Shia"]),
    "Nation of Islam": (0.5, ["NATION OF ISLAM", "NOI"]),
    "Jewish": (1, ["JEWISH", "Jewish", "Judaism"]),
    "Buddhist": (0.8, ["BUDDHIST", "Buddhism", "Zen"]),
    "Hindu": (0.3, ["HINDU", "Hinduism"]),
    "Native American": (1, ["NATIVE AMERICAN", "Native American Spirituality", "NAS"]),
    "Wiccan/Pagan": (0.8, ["WICCA", "Wiccan", "Pagan"]),
    "Atheist/Agnostic": (2, ["ATHEIST", "Atheist", "Agnostic"]),
    "Other": (1, ["OTHER", "Other"]),
    "No preference": (14, ["NONE", "None", "NO PREF", "No Preference", "NO RELIGION"]),
    "Declined": (2, ["DECLINED", "Refused"]),
    "Unknown": (2, ["UNKNOWN", "UNK", "N/A", "?"]),
}
RELIGION_JUNK = ["JEDI", "ASGARDIAN", "SEE CHAPLAIN"]

# Fictional villain organizations only; aliases as different source screens abbreviate them
STG_GROUPS = {
    "HYDRA": ["HYDRA", "Hydra", "H.Y.D.R.A.", "HYD"],
    "A.I.M.": ["AIM", "A.I.M.", "Advanced Idea Mechanics"],
    "Legion of Doom": ["LEGION OF DOOM", "Legion of Doom", "LOD", "L.O.D."],
    "Hellfire Club": ["HELLFIRE CLUB", "Hellfire Club", "HFC"],
    "Brotherhood of Mutants": ["BROTHERHOOD OF MUTANTS", "Brotherhood of Evil Mutants", "BROTHERHOOD", "BOM"],
    "Masters of Evil": ["MASTERS OF EVIL", "MOE"],
    "Sinister Six": ["SINISTER SIX", "Sinister 6", "SIN6"],
    "Injustice League": ["INJUSTICE LEAGUE", "IJL"],
    "The Hand": ["THE HAND", "Hand"],
    "Serpent Society": ["SERPENT SOCIETY", "Serpent Soc"],
    "Intergang": ["INTERGANG"],
    "League of Assassins": ["LEAGUE OF ASSASSINS", "LOA"],
}
STG_WEIGHTS = [16, 10, 12, 8, 9, 9, 7, 7, 6, 6, 5, 5]
STG_TEMPLATES = {
    "confirmed": ["{a} - CONFIRMED", "CONFIRMED {a}", "{a} (C)"],
    "suspected": ["SUSP {a}", "{a} (SUSPECTED)", "SUSP: {a}", "{a} (S)", "{a} - SUSPECTED"],
    "former": ["FORMER {a}", "{a} - RENOUNCED", "EX-{a}", "{a} (FORMER)"],
    "unverified": ["{a}"],   # a bare group name: nobody recorded the status
}
STG_NONE = (["NONE", "None", "N/A", "NO KNOWN AFFIL", "NO"], [45, 10, 20, 15, 10])
STG_JUNK = ["PENDING REVIEW", "SEE STG OFFICE"]
STG_RATE = 0.11          # share of procedural personas with an affiliation; the famous pool has none

# FBI NIBRS offense codes for every offense description the generator writes
NIBRS = {
    "09B": "Person", "100": "Person", "13A": "Person", "13B": "Person", "13C": "Person",
    "200": "Property", "220": "Property", "26C": "Property", "26G": "Property", "290": "Property",
    "520": "Society", "90C": "Group B", "90J": "Group B", "90Z": "Group B",
}
NIBRS_BY_OFFENSE = {
    "UNAUTH FLIGHT IN RESTRICTED AIRSPACE": "90Z", "SPEEDING >=500 MPH": "90Z", "UNLICENSED USE OF SUPERPOWERS": "90Z",
    "UNL POSS PROHIBITED WEAPON (ALIEN TECH)": "520", "TRESPASS ON SECURE FACILITY": "90J",
    "IMPERSONATION OF PUBLIC SERVANT": "26C", "BREACH OF COMPUTER SECURITY": "26G", "TAMPER W/EVID (TIME TRAVEL)": "90Z",
    "UNAUTH INTERDIMENSIONAL TRAVEL": "90Z", "EVADING ARREST (FLIGHT)": "90Z", "OBSTRUCT HWY W/WEBBING": "90C",
    "CRIM MISCHIEF >=$150K<$300K": "290", "CRIM MISCHIEF >=$300K": "290", "CRIM MISCHIEF >=$300K, HABITUAL": "290",
    "ARSON (PYROKINETIC)": "200", "RECKLESS DAMAGE BY ENERGY BLAST": "290", "DESTRUCTION OF INFRASTRUCTURE (BRIDGE)": "290",
    "BURGLARY OF VILLAIN LAIR": "220", "CRIM MISCHIEF BY WEATHER EVENT >=$150K<$300K": "290",
    "VIGILANTISM W/GRAPPLING DEVICE": "13B", "ASSAULT ON HENCHMAN": "13B", "UNLAWFUL RESTRAINT OF SUSPECT": "100",
    "COERCED CONFESSION W/TELEPATHY": "13C", "ASSAULT PUB SERV (MISTAKEN IDENTITY)": "13B",
    "AGG ASSAULT W/SUPERPOWER": "13A", "AGG ASSAULT W/DEADLY WEAPON": "13A", "AGG ASSAULT W/ADAMANTIUM CLAWS": "13A",
    "MANSLAUGHTER (COLLATERAL DAMAGE)": "09B",
    # the ten reviewed rows use their own wording
    "RECKLESS DAMAGE BY HEAT VISION >=$300K": "290", "COERCED CONFESSION W/LASSO": "13C",
    "SPEEDING >=700 MPH IN SCHOOL ZONE": "90Z", "UNL POSS PROHIBITED WEAPON (POWER RING)": "520",
    "UNAUTH ENTRY RESTRICTED AIRSPACE": "90Z",
}
OFFENSE_TYPED_AS_CODE = ["AGG ASSAULT", "CRIM MISCHIEF", "ASSAULT", "TRESPASS"]
OFFENSE_JUNK = ["13X", "999", "XXX"]

VIOLENCE_VARIANTS = {
    "NV": ["nv", "N/V", "NON-VIOLENT", "Nonviolent", "N-V"],
    "V1": ["v1", "V-1", "V 1", "1"], "V2": ["v2", "V-2", "V 2", "2"], "V3": ["v3", "V-3", "V 3", "3"],
}
VIOLENCE_JUNK = ["V4", "VV2", "?"]

# Accented spellings of surnames in the procedural pool (source screens differ on accents)
ACCENTED = {n.replace("Á", "A").replace("É", "E").replace("Í", "I").replace("Ó", "O").replace("Ú", "U")
            .replace("Ñ", "N"): n for n in
            ["GARCÍA", "RODRÍGUEZ", "MARTÍNEZ", "HERNÁNDEZ", "LÓPEZ", "GONZÁLEZ", "PÉREZ", "SÁNCHEZ", "RAMÍREZ",
             "GÓMEZ", "DÍAZ", "GUTIÉRREZ", "JIMÉNEZ", "ÁLVAREZ", "VÁSQUEZ", "FERNÁNDEZ", "MUÑOZ", "TREVIÑO",
             "DOMÍNGUEZ", "VÁZQUEZ", "MÉNDEZ", "GARCÍA-LÓPEZ"]}


def _pick(pairs):
    return v4_rng.choices([p[0] for p in pairs], [p[1] for p in pairs])[0]


class DomainState:
    """Per-person race, religion and STG, so a person's records mostly agree (as in real systems)."""

    def __init__(self, famous_sids: set):
        self.people = {}
        self.famous_sids = famous_sids

    def fields(self, sid: str, offense: str):
        """Values for race, religion, stg_affiliation and offense_code for one record, before blanks."""
        seen = sid in self.people
        if not seen:
            concepts = list(RACE_CONCEPTS)
            person = {
                "race": v4_rng.choices(concepts, [RACE_CONCEPTS[c][0] for c in concepts])[0],
                "religion": v4_rng.choices(list(RELIGION_GROUPS), [g[0] for g in RELIGION_GROUPS.values()])[0],
                "stg": None, "status": None,
            }
            if sid not in self.famous_sids and v4_rng.random() < STG_RATE:
                person["stg"] = v4_rng.choices(list(STG_GROUPS), STG_WEIGHTS)[0]
                person["status"] = v4_rng.choices(["confirmed", "suspected", "unverified", "former"], [35, 30, 25, 10])[0]
            self.people[sid] = person
        person = self.people[sid]

        # race: the same person, spelled however this record's screen spelled it; a few records disagree
        concept = person["race"]
        if seen and v4_rng.random() < 0.012:
            concept = v4_rng.choice([c for c in ("W", "B", "H") if c != concept])
        race = v4_rng.choice(RACE_JUNK) if v4_rng.random() < 0.0005 else _pick(RACE_CONCEPTS[concept][1])

        # religion: people can change faith between incarcerations; that's not an error
        if seen and v4_rng.random() < 0.08:
            person["religion"] = v4_rng.choices(list(RELIGION_GROUPS), [g[0] for g in RELIGION_GROUPS.values()])[0]
        religion = (v4_rng.choice(RELIGION_JUNK) if v4_rng.random() < 0.001
                    else v4_rng.choice(RELIGION_GROUPS[person["religion"]][1]))

        # STG: status can move between records (suspected -> confirmed -> former)
        if person["stg"] and seen and v4_rng.random() < 0.2:
            person["status"] = {"suspected": "confirmed", "unverified": "confirmed",
                                "confirmed": "former", "former": "former"}[person["status"]]
        if v4_rng.random() < 0.0005:
            stg = v4_rng.choice(STG_JUNK)
        elif person["stg"]:
            alias = v4_rng.choice(STG_GROUPS[person["stg"]])
            if v4_rng.random() < 0.003:
                other = v4_rng.choice([g for g in STG_GROUPS if g != person["stg"]])
                alias = f"{alias}/{v4_rng.choice(STG_GROUPS[other])}"   # two groups in one cell
            stg = v4_rng.choice(STG_TEMPLATES[person["status"]]).format(a=alias)
        else:
            stg = v4_rng.choices(*STG_NONE)[0]

        return [race, religion, stg, offense_code_as_written(NIBRS_BY_OFFENSE[offense])]


def offense_code_as_written(code: str) -> str:
    roll = v4_rng.random()
    if roll < 0.005:
        return v4_rng.choice([c for c in NIBRS if c != code])          # valid code, wrong offense
    if roll < 0.017:
        return v4_rng.choice(OFFENSE_TYPED_AS_CODE)                     # description typed into the code field
    if roll < 0.020:
        return v4_rng.choice(OFFENSE_JUNK)
    if code.startswith("0") and v4_rng.random() < 0.4:
        return code[1:]                                                 # 09B -> 9B: the leading zero dropped
    if roll < 0.050 and code[-1].isalpha():
        return code.lower()
    if roll < 0.080:
        if code.isdigit():
            return code + ".0"                                          # opened and saved in Excel
        return code[:-1] + v4_rng.choice(["-", " "]) + code[-1]
    return code


def dirty_violence(value: str) -> str:
    """Only non-blank, canonical values get respelled; blanks stay blank."""
    if value not in VIOLENCE_VARIANTS:
        return value
    roll = v4_rng.random()
    if roll < 0.001:
        return v4_rng.choice(VIOLENCE_JUNK)
    if roll < 0.06:
        return v4_rng.choice(VIOLENCE_VARIANTS[value])
    return value


def _mojibake(text: str):
    """How UTF-8 text looks after a system reads it as Windows-1252. None if that can't happen cleanly."""
    try:
        return text.encode("utf-8").decode("cp1252")
    except UnicodeDecodeError:
        return None


def accent_name(cased: str) -> str:
    """Some screens keep Spanish accents, some don't; a few records carry encoding damage too."""
    base = cased.upper()
    if base not in ACCENTED or v4_rng.random() >= 0.30:
        return cased
    accented = ACCENTED[base]
    out = "".join(a.lower() if c.islower() else a for c, a in zip(cased, accented))
    if v4_rng.random() < 0.08:
        damaged = _mojibake(out)
        if damaged and all(ch in "ÃÂ‘“‰±³©¡º\xad" or ord(ch) < 128 for ch in damaged):
            return damaged
    return out


def dirty_note(note: str) -> str:
    """Web-form and encoding damage. Every change here is undone exactly by session 2's text repair."""
    roll = v4_rng.random()
    if roll < 0.03 and ". " in note:
        return note.replace(". ", ".<br>", 1)
    if roll < 0.05 and " " in note:
        return note.replace(" ", "&nbsp;", 1)
    if roll < 0.08 and " " in note:
        i = v4_rng.choice([i for i, ch in enumerate(note) if ch == " "])
        return note[:i] + "  " + note[i + 1:]
    if roll < 0.10:
        return " " + note + "  "
    if roll < 0.13 and ("'" in note or " - " in note):
        smart = note.replace("'", "’").replace(" - ", " – ")
        return (_mojibake(smart) or smart) if v4_rng.random() < 0.5 else smart
    return note


def v4_nulls(values: list) -> list:
    out = []
    for v in values:
        roll = v4_rng.random()
        if roll < TRUE_NULL_RATE:
            out.append("")
        elif roll < TRUE_NULL_RATE + PADDED_NULL_RATE:
            out.append(" " * v4_rng.randint(1, 8))
        else:
            out.append(v)
    return out


def assemble(v1: list, domains: list, extra: list, flags: list) -> list:
    """v1 (16 columns) + v4 domains + v2 + flags, in HEADER_OUT order; applies the v4 damage."""
    v1 = list(v1)
    if v1[2].strip():
        v1[2] = accent_name(v1[2])
    v1[9] = dirty_violence(v1[9])
    note = extra[4]
    if note.strip():
        note = dirty_note(note)
    race, religion, stg, offense_code = domains
    return (v1[0:7] + [race, religion] + v1[7:11] + [stg] + v1[11:13] + [offense_code] + v1[13:16]
            + extra[:4] + flags + [note])


def generate(rows: int = DEFAULT_ROWS, out: str = DEFAULT_OUT, seed: int = DEFAULT_SEED,
             answer_key: str = None) -> dict:
    """Write the dataset (and the conduct-notes answer key). Same seed = identical files."""
    rng.seed(seed)
    case_rng.seed(seed + 1)
    v2_rng.seed(seed + 2)
    v3_rng.seed(seed + 3)
    v4_rng.seed(seed + 4)
    if answer_key is None:
        answer_key = str(out).replace(".csv", "") + "_answer_key.csv"
    ROWS = rows
    famous = parse_famous()
    blocked = {f["alias"] for f in famous} | {"GREEN ARROW", "BLACK WIDOW", "IRON FIST", "CAPTAIN AMERICA"}
    reviewed_sid = {(r[4], r[2].upper()): r[1] for r in REVIEWED}
    reviewed_tdcj = {int(r[0]) for r in REVIEWED}
    reviewed_sids = {int(r[1]) for r in REVIEWED if r[1]}

    sid_iter = (x for x in rng.sample(range(4_000_000, 9_000_000), ROWS + 50) if x not in reviewed_sids)
    records = []

    # Famous personas: 1-3 incarcerations each, same SID every time
    for f in famous:
        f["sid"] = reviewed_sid.get((f["alias"], f["last"])) or f"{next(sid_iter):08d}"
        records += build_records(f, rng.choices([1, 2, 3], [55, 30, 15])[0])

    # Procedural personas until the target row count is reached
    famous_aliases = [f["alias"] for f in famous]
    target = ROWS - len(REVIEWED)
    while len(records) < target:
        gender = "F" if rng.random() < 0.22 else "M"
        powers, threat = procedural_powers()
        persona = {
            "sid": f"{next(sid_iter):08d}", "last": rng.choice(LAST_NAMES),
            "first": rng.choice(FEMALE_FIRST if gender == "F" else MALE_FIRST),
            "alias": rng.choice(famous_aliases) if rng.random() < COPYCAT_RATE
            else procedural_alias(gender, blocked),
            "gender": gender, "city": rng.choice(CITIES), "powers": powers, "threat": threat,
            "dob": None,
        }
        n = rng.choices([1, 2, 3], [90, 8, 2])[0]
        records += build_records(persona, min(n, target - len(records)))

    # TDCJ numbers are issued in sentence-date order, one per incarceration
    records.sort(key=lambda r: r["sentence"])
    tdcj_numbers = sorted(x for x in rng.sample(range(1_000_000, 2_500_000), len(records) + 50)
                          if x not in reviewed_tdcj)[: len(records)]
    for rec, num in zip(records, tdcj_numbers):
        rec["tdcj"] = f"{num:08d}"
    rng.shuffle(records)

    key_rows = []
    state = DomainState({f["sid"] for f in famous})
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(HEADER_OUT)
        for r in REVIEWED:
            extra, label = v2_fields(r[8], r[9], r[11], r[12], r[6], r[4])
            extra = v2_nulls(extra)
            flags = v3_nulls(flag_fields(r[10], r[11]))
            domains = v4_nulls(state.fields(r[1], r[12]))
            w.writerow(assemble(r, domains, extra, flags))
            key_rows.append((r[0], label if extra[-1].strip() else "no_note"))
        for r in records:
            row = [r["tdcj"], r["sid"], mix_case(r["last"]), mix_case(r["first"]), r["alias"],
                   fmt_date(r["dob"]), r["gender"], r["city"], r["custody"], r["vcode"], r["threat"],
                   r["powers"], r["offense"], r["housing"], fmt_date(r["sentence"]), fmt_date(r["release"])]
            for i in range(1, len(row)):
                roll = rng.random()
                if roll < TRUE_NULL_RATE:
                    row[i] = ""
                elif roll < TRUE_NULL_RATE + PADDED_NULL_RATE:
                    row[i] = padded_null()
            extra, label = v2_fields(r["custody"], r["vcode"], r["powers"], r["offense"], r["gender"], r["alias"])
            extra = v2_nulls(extra)
            flags = v3_nulls(flag_fields(r["threat"], r["powers"]))
            domains = v4_nulls(state.fields(r["sid"], r["offense"]))
            w.writerow(assemble(row, domains, extra, flags))
            key_rows.append((r["tdcj"], label if extra[-1].strip() else "no_note"))

    with open(answer_key, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["tdcj_number", "conduct_label"])
        w.writerows(key_rows)
    return {"data": str(out), "answer_key": str(answer_key), "rows": rows, "seed": seed}


if __name__ == "__main__":
    args = sys.argv[1:]
    result = generate(
        rows=int(args[0]) if len(args) > 0 else DEFAULT_ROWS,
        out=args[1] if len(args) > 1 else DEFAULT_OUT,
        seed=int(args[2]) if len(args) > 2 else DEFAULT_SEED,
        answer_key=args[3] if len(args) > 3 else None,
    )
    print(f"Wrote {result['rows']:,} rows to {result['data']} (answer key: {result['answer_key']})")
