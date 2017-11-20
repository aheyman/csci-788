import enchant
from pymongo import MongoClient
from pymongo import InsertOne, DeleteMany, UpdateOne
from bson.objectid import ObjectId
import datetime
import re
from nltk import Text
from nltk.tokenize import word_tokenize
from wordsegment import load, segment

def enum_caster(x):
    return {
        'Recommends': 1,
        'Doesn\'t Recommend': -1,
        'Positive Outlook':1,
        'Neutral Outlook':0,
        'Negative Outlook':-1,
        'Approves of CEO':1,
        'No opinion of CEO':0,
        'Disapproves of CEO':-1
    }[x]

def xstr(s):
    if s is None:
        return ''
    return str(s)

# These companies have < 50 total reviews
bad_companies = [
    "RPM-International",
    "Pinnacle-West-Capital",
    "Western-Refining",
    "Primoris-Services",
    "Northern-Tier-Energy",
    "TransDigm-Group-Incorporated",
    "Triple-S-Management",
    "Graham-Holdings",
    "Metaldyne-Performance-Group",
    "Roper-Technologies",
    "Alon-USA-Energy",
    "Fortune-Brands-Home-and-Security",
    "Lansing-Trade-Group-LLC",
    "Antero-Resources",
    "UGI",
    "MDU-Resources-Group",
    "Sprague-Resources",
    "Affiliated-Managers-Group",
    "Watsco",
    "Green-Plains",
    "Providence-Service",
    "PriceSmart",
    "CalAtlantic-Group",
    "Reliance-Steel-and-Aluminum-Co",
    "Century-Aluminum",
    "Prologis",
    "Magellan-Midstream-Partners",
    "Hyster-Yale-Materials-Handling",
    "WEC-Energy-Group",
    "Edison-International",
    "Tailored-Brands",
    "TECO-Energy",
    "MDC-Holdings",
    "QEP-Resources",
    "Whiting-Petroleum",
    "Mueller-Industries",
    "Targa-Resources-Corp",
    "PBF-Energy",
    "Welltower",
    "Kansas-City-Southern",
    "Seaboard",
    "Continental-Resources",
    "Twenty-First-Century-Fox",
    "Delek-US-Holdings",
    "Greenbrier",
    "Telephone-and-Data-Systems",
    "Vectren",
    "On-Assignment",
    "Steel-Dynamics",
    "OGE-Energy-Corp",
    "Calumet-Specialty-Products-Partners",
    "WPX-Energy",
    "Vista-Outdoor",
    "NuStar-Energy",
    "California-Resources",
    "Westar-Energy",
    "KAR-Auction-Services",
    "AmSurg-Corp",
    "Westlake-Chemical",
    "Talen-Energy",
    "Vornado-Realty-Trust",
    "ArcBest",
    "Buckeye-Partners",
    "EP-Energy",
    "Enable-Midstream-Partners",
    "Dycom-Industries",
    "SPX-FLOW"]

count = 0

# word segement
load()
d = enchant.Dict("en_US")

### DB connections
client = MongoClient()
glassdoor_db = client.test
glassdoor_reviews = glassdoor_db.reviews

companies = glassdoor_reviews.distinct('company')
lowercase_fields = ['review_title','job_title','duration','pros','cons','advice_mgmt']
enumerate_fields = ['recommend', 'outlook', 'ceo']
pattern = re.compile(r"\((\d+)\)") # parsing out the helpful values

operations = []

# Duplicate Removal
for doc in glassdoor_reviews.aggregate([
                            {'$group':{'_id':'$url',
                                       'dups':{'$push':'$_id'},
                                       'count': {'$sum': 1}}
                            },
                            {'$match':{'count': {'$gt': 1}}}
                            ]):

    # Skip the first element in the list
    for dup in doc['dups'][1:]:
        operations.append(
            DeleteMany({"_id": ObjectId(dup)})
        )


for idx, company in enumerate(companies):
    print("Working on number: " + str(idx) + ", Name: " + company)


    for doc in glassdoor_reviews.find({'company':{'$eq':company}}):

        quarter = ((doc['review_date'].month-1)//3 + 1)
        operations.extend([
                UpdateOne({"_id": doc["_id"]}, {"$set": {'quarter': int(quarter)}})
                ])

        # Remove companies that have < 50 reviews
        if doc['company'] in bad_companies:
            operations.append(
                    DeleteMany({"_id": doc["_id"]})
                    )

        # Lowercase
        for field in lowercase_fields:
            if doc[field] is not None:
                operations.append(
                    UpdateOne({"_id": doc["_id"]}, {"$set": {field: doc[field].lower()}})
                )

        # dupes in Duration
        if doc['duration'] == doc['pros']:
            operations.append(
                     UpdateOne({"_id": doc["_id"]}, {"$set": {'duration': None}})
                 )

        # If the review contains 3 or more long, misspelled words, segment
        words = Text(word_tokenize(xstr(doc.get('pros')) + xstr(doc.get('cons')) + xstr(doc.get('advice_mgmt'))))
        if len([w for w in set(words) if len(w) > 12 and w.isalpha() and not (d.check(w))]) > 2:
            pros = ' '.join(word for word in segment(xstr(doc['pros'])))
            cons = ' '.join(word for word in segment(xstr(doc['cons'])))
            mgmt = ' '.join(word for word in segment(xstr(doc['advice_mgmt'])))
            operations.extend([
                UpdateOne({"_id": doc["_id"]}, {"$set": {'pros': pros}}),
                UpdateOne({"_id": doc["_id"]}, {"$set": {'cons': cons}}),
                UpdateOne({"_id": doc["_id"]}, {"$set": {'advice_mgmt': mgmt}})
                ]
            )

        # convert enums
        for enum in enumerate_fields:
            if doc[enum] is not None:
                val = enum_caster(doc[enum])
                operations.append(
                    UpdateOne({"_id": doc["_id"]}, {"$set": {enum: val}})
                )

        # convert to date
        if doc['review_date'] is not None:
            date = datetime.datetime.strptime(doc['review_date'],'%Y-%m-%d')
            operations.append(
                UpdateOne({"_id": doc["_id"]}, {"$set": {'review_date': date}})
            )

        # pase out helpful as a numerical value
        if doc['helpful'] is not None:
            val = pattern.findall(doc['helpful'])[0]
            operations.append(
                UpdateOne({"_id": doc["_id"]}, {"$set": {'helpful': int(val)}})
            )

        # Send once every 1000 in batch
        if (len(operations) == 1000):
            glassdoor_reviews.bulk_write(operations, ordered=False)
            operations = []

    if (len(operations) > 0):
        glassdoor_reviews.bulk_write(operations, ordered=False)



