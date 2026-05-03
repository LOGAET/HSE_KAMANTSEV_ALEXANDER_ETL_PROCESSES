from pymongo import MongoClient
from faker import Faker
import random, uuid
from datetime import datetime, timedelta

fake = Faker('ru_RU')
client = MongoClient('mongodb://admin:admin@localhost:27017/')
db = client['etl_db']

USER_IDS = [f'user_{i}' for i in range(1, 201)]

sessions = []
for _ in range(1000):
    start = fake.date_time_between(start_date='-90d', end_date='now')
    sessions.append({
        'session_id': f'sess_{uuid.uuid4().hex[:8]}',
        'user_id': random.choice(USER_IDS),
        'start_time': start,
        'end_time': start + timedelta(minutes=random.randint(1, 120)),
        'pages_visited': random.sample(['/home', '/products', '/cart', '/profile', '/checkout'], k=random.randint(1, 5)),
        'device': random.choice(['mobile', 'desktop', 'tablet']),
        'actions': random.sample(['login', 'view_product', 'add_to_cart', 'remove_from_cart', 'logout'], k=random.randint(1, 4))
    })
db.UserSessions.insert_many(sessions)

events = []
for _ in range(2000):
    events.append({
        'event_id': f'evt_{uuid.uuid4().hex[:8]}',
        'timestamp': fake.date_time_between(start_date='-90d', end_date='now'),
        'event_type': random.choice(['click', 'scroll', 'purchase', 'search', 'error']),
        'details': random.choice(['/products/42', '/home', '/cart', '/checkout'])
    })
db.EventLogs.insert_many(events)

statuses = ['open', 'closed', 'pending', 'resolved']
issue_types = ['payment', 'delivery', 'account', 'product', 'refund']
tickets = []
for i in range(500):
    created = fake.date_time_between(start_date='-90d', end_date='-1d')
    tickets.append({
        'ticket_id': f'ticket_{i+1:04d}',
        'user_id': random.choice(USER_IDS),
        'status': random.choice(statuses),
        'issue_type': random.choice(issue_types),
        'messages': [
            {'sender': 'user', 'message': fake.sentence(), 'timestamp': created},
            {'sender': 'support', 'message': fake.sentence(), 'timestamp': created + timedelta(hours=1)}
        ],
        'created_at': created,
        'updated_at': created + timedelta(hours=random.randint(1, 72))
    })
db.SupportTickets.insert_many(tickets)

recs = []
for uid in USER_IDS:
    recs.append({
        'user_id': uid,
        'recommended_products': [f'prod_{random.randint(100, 999)}' for _ in range(random.randint(3, 8))],
        'last_updated': fake.date_time_between(start_date='-7d', end_date='now')
    })
db.UserRecommendations.insert_many(recs)

reviews = []
for i in range(300):
    reviews.append({
        'review_id': f'rev_{i+1:04d}',
        'user_id': random.choice(USER_IDS),
        'product_id': f'prod_{random.randint(100, 999)}',
        'review_text': fake.text(max_nb_chars=200),
        'rating': random.randint(1, 5),
        'moderation_status': random.choice(['pending', 'approved', 'rejected']),
        'flags': random.sample(['contains_images', 'spam', 'offensive', 'verified_purchase'], k=random.randint(0, 2)),
        'submitted_at': fake.date_time_between(start_date='-30d', end_date='now')
    })
db.ModerationQueue.insert_many(reviews)

print("Данные успешно сгенерированы!")
for col in ['UserSessions', 'EventLogs', 'SupportTickets', 'UserRecommendations', 'ModerationQueue']:
    print(f"  {col}: {db[col].count_documents({})} документов")
