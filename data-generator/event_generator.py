import random
from datetime import datetime, timezone

CATEGORIES = ["electronics", "clothing", "home", "books", "sports", "toys"]
PRODUCTS = {
    "electronics": ["p_1001", "p_1002", "p_1003", "p_1004", "p_1005"],
    "clothing": ["p_2001", "p_2002", "p_2003", "p_2004", "p_2005"],
    "home": ["p_3001", "p_3002", "p_3003", "p_3004", "p_3005"],
    "books": ["p_4001", "p_4002", "p_4003", "p_4004", "p_4005"],
    "sports": ["p_5001", "p_5002", "p_5003", "p_5004", "p_5005"],
    "toys": ["p_6001", "p_6002", "p_6003", "p_6004", "p_6005"],
}

SEARCH_QUERIES = [
    "wireless headphones", "running shoes", "coffee maker",
    "python books", "yoga mat", "laptop stand",
    "winter jacket", "desk lamp",
]

def generate_event():
    timestamp = datetime.now(timezone.utc).isoformat()
    user_id = f"u_{random.randint(10000, 99999)}"
    session_id = f"s_{random.randint(10000, 99999)}"
    event_type = random.choices(
        ["page_view", "add_to_cart", "remove_from_cart", "purchase", "search"],
        weights=[50, 20, 10, 10, 10],
        k=1
    )[0]

    event = {
        "timestamp": timestamp,
        "user_id": user_id,
        "session_id": session_id,
        "event_type": event_type,
        "product_id": None,
        "quantity": None,
        "price": None,
        "category": None,
        "search_query": None,
    }

    if event_type == "search":
        event["search_query"] = random.choice(SEARCH_QUERIES)
    else:
        category = random.choice(CATEGORIES)
        product_id = random.choice(PRODUCTS[category])
        quantity = random.randint(1, 5)
        price = round(random.uniform(9.99, 299.99), 2)

        event["product_id"] = product_id
        event["category"] = category

        if event_type in ["add_to_cart", "remove_from_cart", "purchase"]:
            event["quantity"] = quantity
            event["price"] = price

    return event

def generate_events(n):
    return [generate_event() for _ in range(n)]
