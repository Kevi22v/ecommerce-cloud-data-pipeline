# src/generator/catalog.py

# 5 Locations with weights (New York gets ~40% of sales, Miami gets ~5%)
LOCATIONS = ["New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX", "Miami, FL"]
LOCATION_WEIGHTS = [0.40, 0.25, 0.15, 0.15, 0.05]

CATALOG = {
    "Electronics": [
        {"name": "Gaming Laptop", "price": 1200.00, "weight": 2},   # Rare
        {"name": "Smartphone", "price": 800.00, "weight": 5},
        {"name": "Smart TV", "price": 600.00, "weight": 8},
        {"name": "Wireless Headphones", "price": 150.00, "weight": 20},
        {"name": "Smartwatch", "price": 200.00, "weight": 15}
    ],
    "Apparel": [
        {"name": "Graphic T-Shirt", "price": 20.00, "weight": 80},  # Very Common
        {"name": "Denim Jeans", "price": 50.00, "weight": 60},
        {"name": "Running Sneakers", "price": 80.00, "weight": 40},
        {"name": "Winter Jacket", "price": 100.00, "weight": 30},
        {"name": "Leather Belt", "price": 30.00, "weight": 70}
    ],
    "Home & Kitchen": [
        {"name": "Coffee Maker", "price": 100.00, "weight": 40},
        {"name": "Blender", "price": 50.00, "weight": 60},
        {"name": "Microwave", "price": 150.00, "weight": 20},
        {"name": "Toaster", "price": 30.00, "weight": 80},           # Very Common
        {"name": "Air Fryer", "price": 120.00, "weight": 30}
    ],
    "Books & Media": [
        {"name": "Sci-Fi Novel", "price": 15.00, "weight": 90},     # Almost every cart!
        {"name": "Python Programming Guide", "price": 45.00, "weight": 40},
        {"name": "Gourmet Cookbook", "price": 30.00, "weight": 60},
        {"name": "Strategy Board Game", "price": 55.00, "weight": 35},
        {"name": "Vintage Vinyl Record", "price": 25.00, "weight": 50}
    ]
}