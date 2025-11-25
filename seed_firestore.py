# seed_firestore.py
# Final seeding script - Khichdi + 20 real professional recipes + users + interactions
# Make sure serviceAccountKey.json is in the same folder
# Run: python seed_firestore.py

import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import uuid, random, time

# Initialize Firebase Admin
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

print("Connected to Firestore.")

# Helper to upload and print
def upload(collection, doc_id, data):
    db.collection(collection).document(doc_id).set(data)

# Common metadata pools
nutrition_groups_list = [
    "Carbs", "Protein", "Fat", "Fiber",
    "Vitamins", "Minerals", "Sugars", "Antioxidants"
]

occasion_pool = [
    "Breakfast", "Lunch", "Dinner", "Snack",
    "Festive", "Winter Special", "Summer Cooler",
    "Monsoon Snack", "Sick Day Comfort"
]

category_types = [
    "Comfort Food", "Spicy", "Sweet", "Savory",
    "Healthy", "High Protein", "Street Food", "Creamy Gravy"
]

texture_pool = ["Soft", "Crispy", "Creamy", "Crunchy", "Liquid", "Chewy"]
temperature_pool = ["Hot", "Warm", "Cold"]
synthetic_ids = []

# ---------- 1) Khichdi (your seed recipe) ----------
khichdi_id = "khichdi-" + uuid.uuid4().hex[:8]
khichdi = {
    "title": "Khichdi",
    "author": "Sudarshan Chole",
    "cuisine": "Indian",
    "difficulty": "easy",
    "servings": 3,
    "prep_time_min": 20,
    "cook_time_min": 25,
    "total_time_min": 45,
    "tags": ["comfort","vegetarian","one-pot"],
    "created_at": datetime.utcnow(),

    "occasion": ["Dinner", "Sick Day Comfort"],
    "category_type": "Comfort Food",
    "texture": "Soft",
    "served_temperature": "Hot",
    "nutrition_groups": ["Carbs","Protein","Fiber"],

    "ingredients": [
        {"name":"Rice","quantity":2,"unit":"cups"},
        {"name":"Moong dal","quantity":0.5,"unit":"cups"},
        {"name":"Toor dal","quantity":50,"unit":"grams"},
        {"name":"Onions","quantity":2,"unit":"medium"},
        {"name":"Tomato","quantity":1,"unit":"small"},
        {"name":"Green chilies","quantity":4,"unit":"pieces"},
        {"name":"Potato","quantity":1,"unit":"small"},
        {"name":"Curry leaves","quantity":10,"unit":"leaves"},
        {"name":"Coriander","quantity":15,"unit":"grams"},
        {"name":"Jeera","quantity":1,"unit":"tsp"},
        {"name":"Mustard seeds","quantity":0.25,"unit":"tsp"},
        {"name":"Turmeric","quantity":0.5,"unit":"tsp"},
        {"name":"Red chilli powder","quantity":1,"unit":"tbsp"},
        {"name":"Ginger garlic paste","quantity":7,"unit":"grams"},
        {"name":"Garam masala","quantity":1,"unit":"tbsp"},
        {"name":"Salt","quantity":1,"unit":"tbsp"},
        {"name":"Oil","quantity":20,"unit":"ml"},
        {"name":"Water","quantity":4,"unit":"cups"}
    ],

    "steps": [
        {"step_number":1,"description":"Wash rice, moong dal and toor dal thoroughly. Soak in water for 15 minutes, then drain."},
        {"step_number":2,"description":"Peel and chop onions; chop tomatoes, green chilies and dice potatoes into small cubes. Keep curry leaves whole and finely chop coriander for garnish."},
        {"step_number":3,"description":"Place a 3–5 L pressure cooker on medium flame. Warm the cooker for 30–60 seconds and add oil; allow it to heat until it shimmers."},
        {"step_number":4,"description":"Add jeera and mustard seeds. Let them splutter for 20–30 seconds but do not burn."},
        {"step_number":5,"description":"Add chopped onions and green chilies. Sauté for 3–4 minutes until onions turn light golden, stirring continuously."},
        {"step_number":6,"description":"Add chopped tomato and cook for 1–2 minutes until tomatoes soften and turn pulpy."},
        {"step_number":7,"description":"Add salt, red chilli powder, turmeric, ginger-garlic paste, curry leaves and potato cubes. Mix and cook for 4–5 minutes to remove raw spice smell."},
        {"step_number":8,"description":"Add soaked and drained rice, moong dal and toor dal. Mix gently to avoid breaking rice grains."},
        {"step_number":9,"description":"Pour 4 cups of water into the cooker and stir once. Check salt and adjust."},
        {"step_number":10,"description":"Close the cooker lid and place the whistle. Cook on medium flame for 3 whistles (~10–12 minutes)."},
        {"step_number":11,"description":"Turn off the gas and allow the pressure to release naturally. Do not open immediately."},
        {"step_number":12,"description":"Open the lid carefully once pressure is released. Add chopped coriander, mix gently and serve hot with ghee or curd."}
    ],
    "notes": "Serve hot. Adjust water quantity for desired khichdi consistency."
}
upload("recipes", khichdi_id, khichdi)
print("Uploaded Khichdi ->", khichdi_id)
time.sleep(0.2)

# ---------- 2) Full list of 20 real professional recipes ----------
# For each recipe: realistic ingredients + steps + metadata
recipes_data = [
    # 1 Paneer Butter Masala (we already made an example earlier)
    {
        "slug":"paneer-butter-masala",
        "title":"Paneer Butter Masala",
        "cuisine":"Indian",
        "difficulty":"medium",
        "servings":4,
        "prep_time_min":15,
        "cook_time_min":25,
        "total_time_min":40,
        "tags":["restaurant-style","vegetarian","gravy"],
        "occasion":["Lunch","Dinner"],
        "category_type":"Creamy Gravy",
        "texture":"Creamy",
        "served_temperature":"Hot",
        "nutrition_groups":["Protein","Fat","Carbs","Vitamins","Minerals"],
        "ingredients":[
            {"name":"Paneer","quantity":250,"unit":"grams"},
            {"name":"Butter","quantity":2,"unit":"tbsp"},
            {"name":"Oil","quantity":1,"unit":"tbsp"},
            {"name":"Onion","quantity":2,"unit":"medium"},
            {"name":"Tomato","quantity":4,"unit":"medium"},
            {"name":"Ginger garlic paste","quantity":1,"unit":"tbsp"},
            {"name":"Cashews","quantity":12,"unit":"pieces"},
            {"name":"Fresh cream","quantity":3,"unit":"tbsp"},
            {"name":"Kashmiri red chilli powder","quantity":1.5,"unit":"tsp"},
            {"name":"Turmeric powder","quantity":0.5,"unit":"tsp"},
            {"name":"Coriander powder","quantity":1,"unit":"tsp"},
            {"name":"Garam masala","quantity":1,"unit":"tsp"},
            {"name":"Kasuri methi","quantity":1,"unit":"tsp"},
            {"name":"Sugar","quantity":1,"unit":"tsp"},
            {"name":"Salt","quantity":1,"unit":"tsp"},
            {"name":"Water","quantity":0.5,"unit":"cup"},
            {"name":"Coriander (garnish)","quantity":2,"unit":"tbsp"},
            {"name":"Cream (garnish)","quantity":1,"unit":"tsp"}
        ],
        "steps":[
            {"step_number":1,"description":"Soak cashews in warm water for 15 minutes and grind to a smooth paste with a tablespoon of water."},
            {"step_number":2,"description":"Heat butter and oil in a heavy-bottom pan on medium flame. Add finely chopped onions and sauté until translucent (4-5 minutes)."},
            {"step_number":3,"description":"Add ginger-garlic paste and sauté for 30 seconds until raw smell disappears."},
            {"step_number":4,"description":"Add tomato puree and cook on medium heat until oil separates from the masala (6-7 minutes)."},
            {"step_number":5,"description":"Add Kashmiri red chilli, turmeric, coriander powder and salt. Mix well and cook for 1 minute."},
            {"step_number":6,"description":"Stir in the cashew paste and 1/2 cup water. Cook for 2-3 minutes on low heat, stirring continuously."},
            {"step_number":7,"description":"Add paneer cubes gently and simmer for 3 minutes so paneer absorbs the gravy flavors."},
            {"step_number":8,"description":"Stir in fresh cream, kasuri methi (crushed) and garam masala. Cook for 1 minute."},
            {"step_number":9,"description":"Add sugar to balance the acidity and taste; adjust salt if necessary."},
            {"step_number":10,"description":"Turn off the heat, garnish with chopped coriander and a drizzle of cream."},
            {"step_number":11,"description":"Serve hot with naan, roti or jeera rice."}
        ]
    },

    # 2 Chicken Biryani (Dum style)
    {
        "slug":"chicken-biryani",
        "title":"Chicken Biryani (Dum Style)",
        "cuisine":"Indian",
        "difficulty":"hard",
        "servings":4,
        "prep_time_min":40,
        "cook_time_min":60,
        "total_time_min":100,
        "tags":["one-pot","festive"],
        "occasion":["Lunch","Dinner","Festive"],
        "category_type":"Spicy",
        "texture":"Layered",
        "served_temperature":"Hot",
        "nutrition_groups":["Carbs","Protein","Fat","Vitamins","Minerals"],
        "ingredients":[
            {"name":"Basmati rice","quantity":2,"unit":"cups"},
            {"name":"Chicken (with bone)","quantity":800,"unit":"grams"},
            {"name":"Onions (fried)","quantity":3,"unit":"medium"},
            {"name":"Yogurt","quantity":1,"unit":"cup"},
            {"name":"Ginger garlic paste","quantity":2,"unit":"tbsp"},
            {"name":"Biryani masala","quantity":2,"unit":"tbsp"},
            {"name":"Turmeric","quantity":0.5,"unit":"tsp"},
            {"name":"Red chilli powder","quantity":1,"unit":"tsp"},
            {"name":"Garam masala","quantity":1,"unit":"tsp"},
            {"name":"Mint leaves","quantity":0.5,"unit":"cup"},
            {"name":"Coriander leaves","quantity":0.5,"unit":"cup"},
            {"name":"Fried onions","quantity":1,"unit":"cup"},
            {"name":"Saffron","quantity":0.001,"unit":"pinch"},
            {"name":"Milk (warm)","quantity":2,"unit":"tbsp"},
            {"name":"Ghee","quantity":2,"unit":"tbsp"},
            {"name":"Salt","quantity":1.5,"unit":"tsp"},
            {"name":"Whole spices (bay leaf,cinnamon,cardamom,cloves)","quantity":1,"unit":"tbsp"},
            {"name":"Oil","quantity":2,"unit":"tbsp"}
        ],
        "steps":[
            {"step_number":1,"description":"Wash and soak basmati rice for 30 minutes."},
            {"step_number":2,"description":"Marinate chicken with yogurt, ginger-garlic paste, biryani masala, red chilli, turmeric and salt for at least 30 minutes."},
            {"step_number":3,"description":"Fry onions until deep golden and set aside. Reserve some for garnish."},
            {"step_number":4,"description":"Par-cook rice in salted boiling water till 70% done; drain and keep aside."},
            {"step_number":5,"description":"In a heavy bottom pan, heat ghee and oil, add whole spices and sauté for 30 seconds."},
            {"step_number":6,"description":"Add marinated chicken and cook until chicken is 80% done and oil separates."},
            {"step_number":7,"description":"Layer half the rice over chicken, sprinkle half of mint, coriander and fried onions. Repeat layers."},
            {"step_number":8,"description":"Dissolve saffron in warm milk and drizzle on top, add ghee."},
            {"step_number":9,"description":"Seal the pan with dough or tight lid and cook on low flame (dum) for 25-30 minutes."},
            {"step_number":10,"description":"Turn off heat and rest for 10 minutes before opening. Gently mix layers and serve hot with raita."}
        ]
    },

    # 3 Veg Hakka Noodles
    {
        "slug":"veg-hakka-noodles",
        "title":"Veg Hakka Noodles",
        "cuisine":"Chinese",
        "difficulty":"easy",
        "servings":2,
        "prep_time_min":15,
        "cook_time_min":10,
        "total_time_min":25,
        "tags":["street-style","quick"],
        "occasion":["Lunch","Dinner","Snack"],
        "category_type":"Savory",
        "texture":"Soft",
        "served_temperature":"Hot",
        "nutrition_groups":["Carbs","Vitamins","Fiber"],
        "ingredients":[
            {"name":"Hakka noodles","quantity":200,"unit":"grams"},
            {"name":"Oil","quantity":2,"unit":"tbsp"},
            {"name":"Garlic","quantity":3,"unit":"cloves"},
            {"name":"Green chillies","quantity":1,"unit":"piece"},
            {"name":"Onion","quantity":1,"unit":"medium"},
            {"name":"Cabbage","quantity":1,"unit":"cup shredded"},
            {"name":"Carrot","quantity":0.5,"unit":"cup julienned"},
            {"name":"Capsicum","quantity":0.5,"unit":"cup julienned"},
            {"name":"Spring onion greens","quantity":2,"unit":"tbsp"},
            {"name":"Soya sauce","quantity":1.5,"unit":"tbsp"},
            {"name":"Vinegar","quantity":1,"unit":"tsp"},
            {"name":"Chilli sauce","quantity":1,"unit":"tsp"},
            {"name":"Salt","quantity":1,"unit":"tsp"},
            {"name":"Pepper","quantity":0.5,"unit":"tsp"}
        ],
        "steps":[
            {"step_number":1,"description":"Boil noodles according to packet instructions with a little oil; drain, rinse in cold water and toss with a drop of oil to prevent sticking."},
            {"step_number":2,"description":"Heat wok, add oil and smashed garlic; sauté quickly."},
            {"step_number":3,"description":"Add sliced onion and green chillies; stir-fry 1 minute until translucent."},
            {"step_number":4,"description":"Add shredded cabbage, carrots and capsicum; stir-fry on high heat for 2-3 minutes so vegetables remain crisp."},
            {"step_number":5,"description":"Add soy sauce, chilli sauce, vinegar, salt and pepper; toss well."},
            {"step_number":6,"description":"Add noodles and toss vigorously on high heat for 1-2 minutes so sauces coat noodles evenly."},
            {"step_number":7,"description":"Finish with spring onion greens and serve immediately."}
        ]
    },

    # 4 Veg Manchurian (Gravy)
    {
        "slug":"veg-manchurian-gravy",
        "title":"Veg Manchurian (Gravy)",
        "cuisine":"Chinese",
        "difficulty":"medium",
        "servings":4,
        "prep_time_min":20,
        "cook_time_min":25,
        "total_time_min":45,
        "tags":[" Indo-Chinese","gravy"],
        "occasion":["Lunch","Dinner"],
        "category_type":"Savory",
        "texture":"Soft",
        "served_temperature":"Hot",
        "nutrition_groups":["Carbs","Fiber","Vitamins"],
        "ingredients":[
            {"name":"Cabbage","quantity":1,"unit":"cup finely shredded"},
            {"name":"Carrot","quantity":0.5,"unit":"cup grated"},
            {"name":"Capsicum","quantity":0.5,"unit":"cup finely chopped"},
            {"name":"Maida / All-purpose flour","quantity":3,"unit":"tbsp"},
            {"name":"Cornflour","quantity":2,"unit":"tbsp"},
            {"name":"Salt","quantity":0.75,"unit":"tsp"},
            {"name":"Pepper","quantity":0.5,"unit":"tsp"},
            {"name":"Oil (for frying)","quantity":500,"unit":"ml"},
            {"name":"Oil (for gravy)","quantity":2,"unit":"tbsp"},
            {"name":"Garlic","quantity":4,"unit":"cloves minced"},
            {"name":"Ginger","quantity":1,"unit":"tsp minced"},
            {"name":"Onion","quantity":1,"unit":"medium chopped"},
            {"name":"Tomato ketchup","quantity":2,"unit":"tbsp"},
            {"name":"Soya sauce","quantity":1,"unit":"tbsp"},
            {"name":"Vinegar","quantity":1,"unit":"tsp"},
            {"name":"Spring onion","quantity":2,"unit":"tbsp chopped"}
        ],
        "steps":[
            {"step_number":1,"description":"Combine shredded cabbage, carrot, capsicum with salt, pepper, cornflour and maida; mix to form a stiff mixture."},
            {"step_number":2,"description":"Shape mixture into small balls (manchurian balls) and deep fry in hot oil until golden brown; drain on paper towel."},
            {"step_number":3,"description":"For gravy, heat 2 tbsp oil in a pan; add minced garlic and ginger and sauté until aromatic."},
            {"step_number":4,"description":"Add chopped onion and sauté until soft; add ketchup, soy sauce and vinegar."},
            {"step_number":5,"description":"Add 1 cup water and bring to a simmer. Mix cornflour with a little water and add to thicken the gravy to desired consistency."},
            {"step_number":6,"description":"Add fried manchurian balls to the gravy and simmer for 2-3 minutes so flavors combine."},
            {"step_number":7,"description":"Garnish with chopped spring onion and serve hot with fried rice or noodles."}
        ]
    },

    # 5 Sweet Corn Soup
    {
        "slug":"sweet-corn-soup",
        "title":"Sweet Corn Soup",
        "cuisine":"Chinese",
        "difficulty":"easy",
        "servings":3,
        "prep_time_min":10,
        "cook_time_min":15,
        "total_time_min":25,
        "tags":["soup","comfort"],
        "occasion":["Lunch","Dinner","Snack"],
        "category_type":"Savory",
        "texture":"Liquid",
        "served_temperature":"Hot",
        "nutrition_groups":["Vitamins","Fiber","Carbs"],
        "ingredients":[
            {"name":"Sweet corn (canned or boiled)","quantity":1,"unit":"cup"},
            {"name":"Vegetable stock or water","quantity":3,"unit":"cups"},
            {"name":"Onion (finely chopped)","quantity":1,"unit":"small"},
            {"name":"Garlic (minced)","quantity":2,"unit":"cloves"},
            {"name":"Carrot (finely chopped)","quantity":0.25,"unit":"cup"},
            {"name":"Capsicum (finely chopped)","quantity":0.25,"unit":"cup"},
            {"name":"Cornflour","quantity":1.5,"unit":"tbsp"},
            {"name":"Egg (optional, beaten)","quantity":1,"unit":"piece"},
            {"name":"Salt","quantity":1,"unit":"tsp"},
            {"name":"Pepper","quantity":0.5,"unit":"tsp"},
            {"name":"Spring onion","quantity":1,"unit":"tbsp chopped"},
            {"name":"Oil","quantity":1,"unit":"tbsp"}
        ],
        "steps":[
            {"step_number":1,"description":"Heat oil in a pot; sauté onions and garlic until soft."},
            {"step_number":2,"description":"Add carrots and capsicum and sauté for 1-2 minutes."},
            {"step_number":3,"description":"Add sweet corn and vegetable stock; bring to a gentle boil."},
            {"step_number":4,"description":"Mix cornflour with 3 tbsp water to make a slurry; slowly add to the soup while stirring to thicken."},
            {"step_number":5,"description":"If using egg, slowly pour beaten egg in a thin stream while stirring to create egg ribbons."},
            {"step_number":6,"description":"Season with salt and pepper, garnish with spring onion and serve hot."}
        ]
    },

    # 6 Tomato Soup
    {
        "slug":"tomato-soup",
        "title":"Tomato Basil Soup",
        "cuisine":"International",
        "difficulty":"easy",
        "servings":3,
        "prep_time_min":10,
        "cook_time_min":20,
        "total_time_min":30,
        "tags":["soup","comfort"],
        "occasion":["Lunch","Dinner"],
        "category_type":"Savory",
        "texture":"Liquid",
        "served_temperature":"Hot",
        "nutrition_groups":["Vitamins","Fiber","Carbs"],
        "ingredients":[
            {"name":"Tomatoes (ripe)","quantity":6,"unit":"medium"},
            {"name":"Onion","quantity":1,"unit":"small"},
            {"name":"Garlic","quantity":2,"unit":"cloves"},
            {"name":"Vegetable stock or water","quantity":3,"unit":"cups"},
            {"name":"Olive oil","quantity":1,"unit":"tbsp"},
            {"name":"Sugar","quantity":0.5,"unit":"tsp"},
            {"name":"Salt","quantity":1,"unit":"tsp"},
            {"name":"Basil leaves (fresh)","quantity":6,"unit":"leaves"},
            {"name":"Cream (optional)","quantity":1,"unit":"tbsp"},
            {"name":"Pepper","quantity":0.25,"unit":"tsp"},
            {"name":"Bread croutons (for serving)","quantity":0.5,"unit":"cup"}
        ],
        "steps":[
            {"step_number":1,"description":"Blanch tomatoes in boiling water for 2 minutes, peel and roughly chop."},
            {"step_number":2,"description":"Heat oil in a saucepan, sauté chopped onions and garlic until translucent."},
            {"step_number":3,"description":"Add chopped tomatoes and cook for 5-6 minutes until soft."},
            {"step_number":4,"description":"Add stock, sugar and basil leaves; simmer for 10 minutes."},
            {"step_number":5,"description":"Blend the soup until smooth, strain if desired, return to pot and adjust seasoning."},
            {"step_number":6,"description":"Stir in a tablespoon of cream if using, garnish with basil and serve with croutons."}
        ]
    },

    # 7 Masala Dosa (batter + making)
    {
        "slug":"masala-dosa",
        "title":"Masala Dosa with Potato Masala",
        "cuisine":"Indian",
        "difficulty":"medium",
        "servings":4,
        "prep_time_min":480,  # includes fermentation
        "cook_time_min":30,
        "total_time_min":510,
        "tags":["breakfast","south-indian"],
        "occasion":["Breakfast","Snack"],
        "category_type":"Savory",
        "texture":"Crispy",
        "served_temperature":"Hot",
        "nutrition_groups":["Carbs","Fiber","Vitamins"],
        "ingredients":[
            {"name":"Parboiled rice","quantity":3,"unit":"cups"},
            {"name":"Urad dal (split)","quantity":1,"unit":"cup"},
            {"name":"Fenugreek seeds","quantity":1,"unit":"tsp"},
            {"name":"Potatoes","quantity":4,"unit":"medium"},
            {"name":"Onion","quantity":1,"unit":"large"},
            {"name":"Mustard seeds","quantity":1,"unit":"tsp"},
            {"name":"Curry leaves","quantity":10,"unit":"leaves"},
            {"name":"Turmeric","quantity":0.5,"unit":"tsp"},
            {"name":"Green chillies","quantity":2,"unit":"pieces"},
            {"name":"Oil/ghee","quantity":3,"unit":"tbsp"},
            {"name":"Salt","quantity":2,"unit":"tsp"},
            {"name":"Water (for batter)","quantity":"as needed","unit":""},
            {"name":"Corriander","quantity":2,"unit":"tbsp"}
        ],
        "steps":[
            {"step_number":1,"description":"Soak rice and urad dal with fenugreek separately for 4–6 hours. Grind to a smooth batter, mix and ferment overnight (8–12 hours)."},
            {"step_number":2,"description":"Boil potatoes, peel and mash lightly; in a pan heat oil, add mustard seeds, curry leaves and onions, sauté until soft."},
            {"step_number":3,"description":"Add turmeric and chopped green chillies, add mashed potatoes, salt and mix; cook 2–3 minutes and finish with coriander."},
            {"step_number":4,"description":"Heat a non-stick tawa, pour a ladle of batter and spread into a thin circle; drizzle oil around edges."},
            {"step_number":5,"description":"When dosa edges lift and bottom is golden, place potato masala on one side and fold; serve hot with sambar and chutney."}
        ]
    },

    # 8 Idli Sambar
    {
        "slug":"idli-sambar",
        "title":"Idli with Sambar",
        "cuisine":"Indian",
        "difficulty":"medium",
        "servings":4,
        "prep_time_min":480,
        "cook_time_min":30,
        "total_time_min":510,
        "tags":["breakfast","south-indian"],
        "occasion":["Breakfast","Lunch"],
        "category_type":"Comfort Food",
        "texture":"Soft",
        "served_temperature":"Hot",
        "nutrition_groups":["Carbs","Protein","Vitamins"],
        "ingredients":[
            {"name":"Idli batter (rice+urad)","quantity":4,"unit":"cups"},
            {"name":"Toor dal","quantity":1,"unit":"cup"},
            {"name":"Tamarind pulp","quantity":0.25,"unit":"cup"},
            {"name":"Sambar powder","quantity":2,"unit":"tbsp"},
            {"name":"Mixed vegetables (drumstick, carrot, onion)","quantity":2,"unit":"cups"},
            {"name":"Mustard seeds","quantity":1,"unit":"tsp"},
            {"name":"Curry leaves","quantity":10,"unit":"leaves"},
            {"name":"Oil","quantity":2,"unit":"tbsp"},
            {"name":"Salt","quantity":1.5,"unit":"tsp"}
        ],
        "steps":[
            {"step_number":1,"description":"Steam idlis from batter in idli moulds for 10–12 minutes until spongy."},
            {"step_number":2,"description":"Cook toor dal until soft and mash or blend to smooth consistency."},
            {"step_number":3,"description":"In a pan, sauté vegetables, add sambar powder and tamarind pulp with water, bring to boil."},
            {"step_number":4,"description":"Add cooked dal, simmer 7–8 minutes, finish with tadka of mustard seeds and curry leaves."},
            {"step_number":5,"description":"Serve hot idlis with sambar and coconut chutney."}
        ]
    },

    # 9 Aloo Paratha
    {
        "slug":"aloo-paratha",
        "title":"Aloo Paratha",
        "cuisine":"Indian",
        "difficulty":"easy",
        "servings":4,
        "prep_time_min":20,
        "cook_time_min":20,
        "total_time_min":40,
        "tags":["breakfast","flatbread"],
        "occasion":["Breakfast","Lunch"],
        "category_type":"Comfort Food",
        "texture":"Soft",
        "served_temperature":"Hot",
        "nutrition_groups":["Carbs","Fat","Vitamins"],
        "ingredients":[
            {"name":"Wheat flour","quantity":3,"unit":"cups"},
            {"name":"Boiled potatoes","quantity":4,"unit":"medium"},
            {"name":"Green chillies","quantity":2,"unit":"pieces"},
            {"name":"Coriander","quantity":2,"unit":"tbsp"},
            {"name":"Garam masala","quantity":0.5,"unit":"tsp"},
            {"name":"Salt","quantity":1,"unit":"tsp"},
            {"name":"Ghee or oil","quantity":3,"unit":"tbsp"},
            {"name":"Water","quantity":"as needed","unit":""}
        ],
        "steps":[
            {"step_number":1,"description":"Make a soft dough with wheat flour and water; rest 15 minutes."},
            {"step_number":2,"description":"Mash boiled potatoes, add chopped chillies, coriander, garam masala and salt."},
            {"step_number":3,"description":"Roll dough into small circle, place potato filling, seal and roll gently into paratha."},
            {"step_number":4,"description":"Cook on tawa with ghee until golden brown on both sides; serve hot with curd or pickle."}
        ]
    },

    # 10 Pav Bhaji
    {
        "slug":"pav-bhaji",
        "title":"Pav Bhaji",
        "cuisine":"Indian",
        "difficulty":"medium",
        "servings":4,
        "prep_time_min":20,
        "cook_time_min":30,
        "total_time_min":50,
        "tags":["street-food","vegan"],
        "occasion":["Lunch","Dinner","Snack"],
        "category_type":"Spicy",
        "texture":"Thick",
        "served_temperature":"Hot",
        "nutrition_groups":["Carbs","Vitamins","Fiber"],
        "ingredients":[
            {"name":"Potatoes","quantity":4,"unit":"medium"},
            {"name":"Cauliflower","quantity":1,"unit":"cup chopped"},
            {"name":"Green peas","quantity":0.5,"unit":"cup"},
            {"name":"Tomato","quantity":4,"unit":"medium"},
            {"name":"Onion","quantity":2,"unit":"medium"},
            {"name":"Pav bhaji masala","quantity":2,"unit":"tbsp"},
            {"name":"Butter","quantity":3,"unit":"tbsp"},
            {"name":"Oil","quantity":1,"unit":"tbsp"},
            {"name":"Lemon","quantity":1,"unit":"piece"},
            {"name":"Salt","quantity":1.5,"unit":"tsp"},
            {"name":"Pav buns","quantity":8,"unit":"pieces"},
            {"name":"Coriander (garnish)","quantity":2,"unit":"tbsp"}
        ],
        "steps":[
            {"step_number":1,"description":"Boil potatoes, cauliflower and peas until soft; mash roughly."},
            {"step_number":2,"description":"In a large pan, heat butter and oil; sauté chopped onions until golden."},
            {"step_number":3,"description":"Add tomato puree and cook until oil separates; add pav bhaji masala and salt."},
            {"step_number":4,"description":"Add mashed vegetables, adjust water and simmer for 8-10 minutes, mashing well to a thick consistency."},
            {"step_number":5,"description":"Serve hot with buttered pav, chopped onions, coriander and lemon wedges."}
        ]
    },

    # 11 Dal Tadka
    {
        "slug":"dal-tadka",
        "title":"Dal Tadka (Yellow Lentils with Tadka)",
        "cuisine":"Indian",
        "difficulty":"easy",
        "servings":4,
        "prep_time_min":10,
        "cook_time_min":25,
        "total_time_min":35,
        "tags":["comfort","vegetarian"],
        "occasion":["Lunch","Dinner"],
        "category_type":"Savory",
        "texture":"Soft",
        "served_temperature":"Hot",
        "nutrition_groups":["Protein","Carbs","Vitamins"],
        "ingredients":[
            {"name":"Toor dal","quantity":1,"unit":"cup"},
            {"name":"Turmeric","quantity":0.5,"unit":"tsp"},
            {"name":"Tomato","quantity":1,"unit":"medium"},
            {"name":"Garlic","quantity":4,"unit":"cloves"},
            {"name":"Ghee","quantity":2,"unit":"tbsp"},
            {"name":"Cumin seeds","quantity":1,"unit":"tsp"},
            {"name":"Red chilli","quantity":1,"unit":"piece"},
            {"name":"Curry leaves","quantity":5,"unit":"leaves"},
            {"name":"Salt","quantity":1,"unit":"tsp"},
            {"name":"Coriander (garnish)","quantity":1,"unit":"tbsp"}
        ],
        "steps":[
            {"step_number":1,"description":"Rinse dal and cook with turmeric and water until soft (pressure cook 3-4 whistles)."},
            {"step_number":2,"description":"Mash dal to desired consistency and simmer for 4-5 minutes."},
            {"step_number":3,"description":"In a small pan, heat ghee, add cumin seeds, slit red chilli, garlic and curry leaves; sauté until garlic is golden."},
            {"step_number":4,"description":"Pour tadka over dal, mix well, garnish with coriander and serve with rice or roti."}
        ]
    },

    # 12 Chole Masala
    {
        "slug":"chole-masala",
        "title":"Chole Masala",
        "cuisine":"Indian",
        "difficulty":"medium",
        "servings":4,
        "prep_time_min":30,
        "cook_time_min":40,
        "total_time_min":70,
        "tags":["north-indian","street-food"],
        "occasion":["Lunch","Dinner"],
        "category_type":"Spicy",
        "texture":"Thick",
        "served_temperature":"Hot",
        "nutrition_groups":["Protein","Carbs","Vitamins"],
        "ingredients":[
            {"name":"Chickpeas (soaked overnight)","quantity":2,"unit":"cups"},
            {"name":"Onion","quantity":2,"unit":"medium"},
            {"name":"Tomato","quantity":3,"unit":"medium"},
            {"name":"Ginger garlic paste","quantity":1,"unit":"tbsp"},
            {"name":"Chole masala","quantity":2,"unit":"tbsp"},
            {"name":"Red chilli powder","quantity":1,"unit":"tsp"},
            {"name":"Amchur (dry mango)","quantity":0.5,"unit":"tsp"},
            {"name":"Oil","quantity":2,"unit":"tbsp"},
            {"name":"Salt","quantity":1.5,"unit":"tsp"},
            {"name":"Coriander","quantity":2,"unit":"tbsp"}
        ],
        "steps":[
            {"step_number":1,"description":"Pressure cook soaked chickpeas with salt until soft (about 6-7 whistles)."},
            {"step_number":2,"description":"Heat oil, add chopped onions and sauté until golden; add ginger-garlic paste and sauté briefly."},
            {"step_number":3,"description":"Add tomato puree and cook until oil separates; add chole masala, red chilli and cook 1-2 minutes."},
            {"step_number":4,"description":"Add cooked chickpeas, some of the cooking water, simmer for 10-12 minutes; add amchur and garnish with coriander."}
        ]
    },

    # 13 Egg Fried Rice
    {
        "slug":"egg-fried-rice",
        "title":"Egg Fried Rice",
        "cuisine":"Chinese",
        "difficulty":"easy",
        "servings":2,
        "prep_time_min":10,
        "cook_time_min":8,
        "total_time_min":18,
        "tags":["quick","lunch"],
        "occasion":["Lunch","Dinner"],
        "category_type":"Savory",
        "texture":"Soft",
        "served_temperature":"Hot",
        "nutrition_groups":["Carbs","Protein"],
        "ingredients":[
            {"name":"Cooked rice (day-old)","quantity":2,"unit":"cups"},
            {"name":"Eggs","quantity":2,"unit":"pieces"},
            {"name":"Oil","quantity":2,"unit":"tbsp"},
            {"name":"Garlic","quantity":2,"unit":"cloves"},
            {"name":"Spring onions","quantity":2,"unit":"tbsp"},
            {"name":"Soy sauce","quantity":1,"unit":"tbsp"},
            {"name":"Salt","quantity":0.5,"unit":"tsp"},
            {"name":"Pepper","quantity":0.25,"unit":"tsp"}
        ],
        "steps":[
            {"step_number":1,"description":"Heat wok, add oil and scramble beaten eggs quickly; remove and set aside."},
            {"step_number":2,"description":"Add more oil, sauté garlic, then add cooked rice and toss on high heat."},
            {"step_number":3,"description":"Add soy sauce, salt, pepper and mix; fold in scrambled eggs and spring onions before serving."}
        ]
    },

    # 14 Chicken Curry
    {
        "slug":"chicken-curry",
        "title":"Home-style Chicken Curry",
        "cuisine":"Indian",
        "difficulty":"medium",
        "servings":4,
        "prep_time_min":15,
        "cook_time_min":40,
        "total_time_min":55,
        "tags":["homestyle","comfort"],
        "occasion":["Lunch","Dinner"],
        "category_type":"Spicy",
        "texture":"Thick",
        "served_temperature":"Hot",
        "nutrition_groups":["Protein","Fat","Vitamins"],
        "ingredients":[
            {"name":"Chicken (cut)","quantity":800,"unit":"grams"},
            {"name":"Onion","quantity":2,"unit":"medium"},
            {"name":"Tomato","quantity":2,"unit":"medium"},
            {"name":"Ginger garlic paste","quantity":1,"unit":"tbsp"},
            {"name":"Turmeric","quantity":0.5,"unit":"tsp"},
            {"name":"Red chilli powder","quantity":1,"unit":"tsp"},
            {"name":"Coriander powder","quantity":1,"unit":"tbsp"},
            {"name":"Garam masala","quantity":1,"unit":"tsp"},
            {"name":"Oil","quantity":3,"unit":"tbsp"},
            {"name":"Salt","quantity":1.5,"unit":"tsp"},
            {"name":"Coriander (garnish)","quantity":2,"unit":"tbsp"}
        ],
        "steps":[
            {"step_number":1,"description":"Heat oil, sauté onions until golden; add ginger-garlic paste and cook until raw smell disappears."},
            {"step_number":2,"description":"Add tomato puree and cook until oil separates."},
            {"step_number":3,"description":"Add spices (turmeric, chilli, coriander) and mix; add chicken pieces and mix to coat."},
            {"step_number":4,"description":"Add water to cover chicken partly, simmer on medium-low for 25-30 minutes until chicken is cooked and gravy thickens."},
            {"step_number":5,"description":"Stir in garam masala, garnish with coriander and serve hot with rice or roti."}
        ]
    },

    # 15 Pasta Alfredo
    {
        "slug":"pasta-alfredo",
        "title":"Pasta Alfredo",
        "cuisine":"Italian",
        "difficulty":"easy",
        "servings":2,
        "prep_time_min":10,
        "cook_time_min":15,
        "total_time_min":25,
        "tags":["italian","comfort"],
        "occasion":["Lunch","Dinner"],
        "category_type":"Creamy",
        "texture":"Creamy",
        "served_temperature":"Hot",
        "nutrition_groups":["Carbs","Fat","Protein"],
        "ingredients":[
            {"name":"Fettuccine or pasta","quantity":200,"unit":"grams"},
            {"name":"Butter","quantity":2,"unit":"tbsp"},
            {"name":"Heavy cream","quantity":1,"unit":"cup"},
            {"name":"Parmesan cheese","quantity":0.75,"unit":"cup grated"},
            {"name":"Garlic (minced)","quantity":2,"unit":"cloves"},
            {"name":"Salt","quantity":0.5,"unit":"tsp"},
            {"name":"Pepper","quantity":0.25,"unit":"tsp"},
            {"name":"Parsley","quantity":1,"unit":"tbsp chopped"}
        ],
        "steps":[
            {"step_number":1,"description":"Cook pasta according to package instructions until al dente; reserve 1/4 cup pasta water."},
            {"step_number":2,"description":"In a pan, melt butter, sauté garlic briefly then add heavy cream and simmer 2 minutes."},
            {"step_number":3,"description":"Stir in grated parmesan until melted and smooth. Add cooked pasta and toss; use reserved pasta water to adjust consistency."},
            {"step_number":4,"description":"Season with salt and pepper, garnish with parsley and serve immediately."}
        ]
    },

    # 16 Veg Burrito
    {
        "slug":"veg-burrito",
        "title":"Veg Burrito",
        "cuisine":"Mexican",
        "difficulty":"easy",
        "servings":2,
        "prep_time_min":15,
        "cook_time_min":10,
        "total_time_min":25,
        "tags":["wrap","quick"],
        "occasion":["Lunch","Snack"],
        "category_type":"Savory",
        "texture":"Soft",
        "served_temperature":"Warm",
        "nutrition_groups":["Carbs","Fiber","Vitamins"],
        "ingredients":[
            {"name":"Tortilla wraps","quantity":2,"unit":"pieces"},
            {"name":"Cooked rice","quantity":1,"unit":"cup"},
            {"name":"Black beans (cooked)","quantity":0.5,"unit":"cup"},
            {"name":"Corn","quantity":0.5,"unit":"cup"},
            {"name":"Bell pepper","quantity":0.5,"unit":"cup chopped"},
            {"name":"Onion","quantity":0.5,"unit":"cup chopped"},
            {"name":"Salsa","quantity":3,"unit":"tbsp"},
            {"name":"Cheese (optional)","quantity":0.5,"unit":"cup grated"},
            {"name":"Lettuce","quantity":0.5,"unit":"cup shredded"}
        ],
        "steps":[
            {"step_number":1,"description":"Warm tortillas on a pan for 20 seconds each to soften."},
            {"step_number":2,"description":"Mix rice, beans, corn, sautéed onion and pepper, salsa and cheese."},
            {"step_number":3,"description":"Place filling on tortilla, fold edges and roll tightly. Serve warm with salsa."}
        ]
    },

    # 17 Chicken Tacos
    {
        "slug":"chicken-tacos",
        "title":"Chicken Tacos",
        "cuisine":"Mexican",
        "difficulty":"easy",
        "servings":4,
        "prep_time_min":15,
        "cook_time_min":15,
        "total_time_min":30,
        "tags":["street-food","quick"],
        "occasion":["Lunch","Snack"],
        "category_type":"Spicy",
        "texture":"Crunchy",
        "served_temperature":"Warm",
        "nutrition_groups":["Protein","Carbs"],
        "ingredients":[
            {"name":"Chicken breast (sliced)","quantity":400,"unit":"grams"},
            {"name":"Taco spice mix","quantity":2,"unit":"tbsp"},
            {"name":"Tortillas","quantity":8,"unit":"pieces"},
            {"name":"Lettuce","quantity":1,"unit":"cup shredded"},
            {"name":"Tomato","quantity":1,"unit":"cup diced"},
            {"name":"Onion","quantity":0.5,"unit":"cup chopped"},
            {"name":"Oil","quantity":2,"unit":"tbsp"},
            {"name":"Sour cream (optional)","quantity":4,"unit":"tbsp"}
        ],
        "steps":[
            {"step_number":1,"description":"Season chicken with taco spice and a pinch of salt; sauté in hot oil until cooked through."},
            {"step_number":2,"description":"Warm tortillas and layer with lettuce, chicken, tomato and onion."},
            {"step_number":3,"description":"Top with sour cream or salsa and serve immediately."}
        ]
    },

    # 18 Chocolate Cake (simple one-bowl)
    {
        "slug":"chocolate-cake",
        "title":"Chocolate Cake (One Bowl)",
        "cuisine":"Dessert",
        "difficulty":"medium",
        "servings":8,
        "prep_time_min":15,
        "cook_time_min":30,
        "total_time_min":45,
        "tags":["dessert","baking"],
        "occasion":["Celebration","Dessert"],
        "category_type":"Sweet",
        "texture":"Soft",
        "served_temperature":"Warm",
        "nutrition_groups":["Carbs","Fat","Sugars"],
        "ingredients":[
            {"name":"All-purpose flour","quantity":1.5,"unit":"cups"},
            {"name":"Sugar","quantity":1,"unit":"cup"},
            {"name":"Cocoa powder","quantity":0.5,"unit":"cup"},
            {"name":"Baking powder","quantity":1,"unit":"tsp"},
            {"name":"Baking soda","quantity":0.5,"unit":"tsp"},
            {"name":"Salt","quantity":0.25,"unit":"tsp"},
            {"name":"Eggs","quantity":2,"unit":"pieces"},
            {"name":"Milk","quantity":0.5,"unit":"cup"},
            {"name":"Vegetable oil","quantity":0.5,"unit":"cup"},
            {"name":"Vanilla extract","quantity":1,"unit":"tsp"},
            {"name":"Hot water","quantity":0.5,"unit":"cup"}
        ],
        "steps":[
            {"step_number":1,"description":"Preheat oven to 180°C (350°F). Grease and line a 8-inch cake pan."},
            {"step_number":2,"description":"In a bowl, sift flour, cocoa, baking powder, baking soda and salt."},
            {"step_number":3,"description":"In another bowl whisk eggs, sugar, oil and vanilla; mix with dry ingredients alternating with milk."},
            {"step_number":4,"description":"Stir in hot water to make a smooth batter; pour into prepared pan and bake for 28-32 minutes until a skewer comes out clean."},
            {"step_number":5,"description":"Cool, remove from pan and frost or dust with cocoa as desired."}
        ]
    },

    # 19 Gulab Jamun
    {
        "slug":"gulab-jamun",
        "title":"Gulab Jamun",
        "cuisine":"Dessert",
        "difficulty":"medium",
        "servings":20,
        "prep_time_min":30,
        "cook_time_min":20,
        "total_time_min":50,
        "tags":["dessert","festive"],
        "occasion":["Festive","Dessert"],
        "category_type":"Sweet",
        "texture":"Soft",
        "served_temperature":"Warm",
        "nutrition_groups":["Carbs","Fats","Sugars"],
        "ingredients":[
            {"name":"Milk powder","quantity":1,"unit":"cup"},
            {"name":"All-purpose flour","quantity":0.25,"unit":"cup"},
            {"name":"Baking soda (a pinch)","quantity":0.01,"unit":"tsp"},
            {"name":"Ghee or oil (for deep fry)","quantity":500,"unit":"ml"},
            {"name":"Milk (to knead)","quantity":0.25,"unit":"cup"},
            {"name":"Sugar","quantity":2,"unit":"cups"},
            {"name":"Water","quantity":1.25,"unit":"cups"},
            {"name":"Cardamom pods","quantity":4,"unit":"pieces"},
            {"name":"Rose water (optional)","quantity":1,"unit":"tsp"}
        ],
        "steps":[
            {"step_number":1,"description":"Make a smooth dough from milk powder, flour and milk; divide into small smooth balls without cracks."},
            {"step_number":2,"description":"Heat sugar and water to make a one-string syrup; add crushed cardamom and a dash of rose water."},
            {"step_number":3,"description":"Deep fry balls on low-medium heat until golden brown; drain briefly and transfer into warm sugar syrup. Let soak for 1-2 hours before serving."}
        ]
    },

    # 20 Kheer (Rice pudding)
    {
        "slug":"kheer",
        "title":"Kheer (Rice Pudding)",
        "cuisine":"Indian",
        "difficulty":"easy",
        "servings":4,
        "prep_time_min":5,
        "cook_time_min":40,
        "total_time_min":45,
        "tags":["dessert","festive"],
        "occasion":["Festive","Dessert"],
        "category_type":"Sweet",
        "texture":"Creamy",
        "served_temperature":"Warm",
        "nutrition_groups":["Carbs","Fats","Proteins"],
        "ingredients":[
            {"name":"Basmati rice","quantity":0.25,"unit":"cup"},
            {"name":"Full cream milk","quantity":1,"unit":"liter"},
            {"name":"Sugar","quantity":0.5,"unit":"cup"},
            {"name":"Cardamom pods","quantity":3,"unit":"pieces"},
            {"name":"Saffron (optional)","quantity":1,"unit":"pinch"},
            {"name":"Cashews & almonds (chopped)","quantity":2,"unit":"tbsp"},
            {"name":"Raisins","quantity":1,"unit":"tbsp"}
        ],
        "steps":[
            {"step_number":1,"description":"Wash rice and soak 15 minutes; drain."},
            {"step_number":2,"description":"Bring milk to a boil in a heavy-bottom pan; add rice and simmer on low, stirring frequently."},
            {"step_number":3,"description":"Cook until rice is soft and milk reduces to creamy consistency (25-30 minutes)."},
            {"step_number":4,"description":"Add sugar, cardamom, saffron, nuts and raisins; simmer 2-3 minutes more and serve warm or chilled."}
        ]
    }
]

# --------------------------------------
# 3. Upload these 20 recipes
# --------------------------------------

synthetic_ids = []   # FIX 1: Create the list before loop

for r in recipes_data:
    rid = f"{r['slug']}-{uuid.uuid4().hex[:6]}"

    doc = {
        "title": r["title"],
        "author": r.get("author","Synthetic Chef"),
        "cuisine": r["cuisine"],
        "difficulty": r["difficulty"],
        "servings": r["servings"],
        "prep_time_min": r["prep_time_min"],
        "cook_time_min": r["cook_time_min"],
        "total_time_min": r["total_time_min"],
        "tags": r.get("tags", []),
        "created_at": datetime.utcnow() - timedelta(days=random.randint(0,400)),

        "occasion": r.get("occasion", random.sample(occasion_pool, k=1)),
        "category_type": r.get("category_type", random.choice(category_types)),
        "texture": r.get("texture", random.choice(texture_pool)),
        "served_temperature": r.get("served_temperature", random.choice(temperature_pool)),
        "nutrition_groups": r.get("nutrition_groups", random.sample(nutrition_groups_list, k=random.randint(2,4))),

        "ingredients": r["ingredients"],
        "steps": r["steps"]
    }

    upload("recipes", rid, doc)
    synthetic_ids.append(rid)   # FIX 2: Add recipe ID for interactions

    print("Uploaded:", r["title"], "->", rid)
    time.sleep(0.08)

print("All 20 recipes uploaded.")

# ----------  Create 10 sample users ----------

# --------------------------------------

print("Creating realistic mixed users...")

user_name_pool = [
    # Indian names
    "Aarav Sharma", "Priya Deshmukh", "Rohan Kulkarni", "Ananya Verma",
    "Kabir Singh", "Meera Nair", "Aditya Patel", "Ishita Pandey",

    # Global names
    "Emily Brown", "Diego Lopez", "Hana Suzuki", "Liam Harris",
    "Sofia Romano", "Omar Hassan", "Mia Johansson",

    # Foodie usernames
    "SpiceQueen", "ChefKaran", "FoodHunter", "NoodleNinja",
    "ButterChickenBoss", "CurryMaster", "TandooriTiger", "SnackAttack"
]

user_ids = []

for i in range(10):
    selected_name = random.choice(user_name_pool)

    # clean user_id format
    user_id = selected_name.lower().replace(" ", "_")

    if user_id in user_ids:
        user_id = f"{user_id}_{uuid.uuid4().hex[:4]}"

    user_ids.append(user_id)

    user_data = {
        "display_name": selected_name,
        "joined_at": datetime.utcnow() - timedelta(days=random.randint(30, 900)),
        "food_preferences": random.choice(["veg", "non-veg", "vegan", "eggetarian"]),
        "city": random.choice(["Mumbai", "Delhi", "Pune", "Bangalore", "Chennai", "Hyderabad"]),
        "account_level": random.choice(["newbie", "regular", "foodie", "superfoodie"])
    }

    db.collection("users").document(user_id).set(user_data)
    print(f"✔ Created user: {user_id}")


# ---------- 4) Create interactions ----------

# Improved Interaction Generator

# ----------------------------

import random
import uuid
from datetime import datetime, timedelta

print("Generating 300 realistic mixed interactions (with trending recipes)...")

# Safety checks (assume these are present earlier in the file)
try:
    all_recipe_ids = [khichdi_id] + synthetic_ids
except NameError:
    raise RuntimeError("all_recipe_ids not found — ensure khichdi_id and synthetic_ids exist above this block.")

if not user_ids:
    raise RuntimeError("user_ids is empty — create users first.")

TARGET_INTERACTIONS = 300
DAYS_SPAN = 400  # spread interactions across last N days

# Choose 3 trending recipes (if less than 3 available, use all)
TRENDING_COUNT = min(3, max(1, len(all_recipe_ids)//7))  # choose sensible number
trending_recipes = random.sample(all_recipe_ids, k=TRENDING_COUNT)

# Persona definitions (probabilities + session behavior)
USER_PERSONAS = {
    "casual_browser": {
        "weights": {"view": 0.9, "like": 0.06, "rating": 0.02, "attempt": 0.02},
        "session_events": (2, 6),
        "trend_bias": 0.05
    },
    "engaged_foodie": {
        "weights": {"view": 0.5, "like": 0.3, "rating": 0.15, "attempt": 0.05},
        "session_events": (4, 12),
        "trend_bias": 0.12
    },
    "home_cook": {
        "weights": {"view": 0.35, "like": 0.15, "rating": 0.2, "attempt": 0.3},
        "session_events": (3, 8),
        "trend_bias": 0.08
    },
    "critic": {
        "weights": {"view": 0.4, "like": 0.05, "rating": 0.45, "attempt": 0.1},
        "session_events": (2, 6),
        "trend_bias": 0.03
    },
    "viral_scroller": {
        "weights": {"view": 0.95, "like": 0.03, "rating": 0.01, "attempt": 0.01},
        "session_events": (15, 40),
        "trend_bias": 0.35
    },
}

persona_keys = list(USER_PERSONAS.keys())

# helper: weighted choice for interaction type
def weighted_choice(weights: dict):
    return random.choices(list(weights.keys()), weights=list(weights.values()), k=1)[0]

# maintain last timestamp per user (so sessions look real)
user_last_ts = {}
# maintain current session_id per user and events in that session
user_session = {}
user_session_count = {}

events_created = 0
counts_by_type = {"view":0, "like":0, "rating":0, "attempt":0}
counts_by_recipe = {rid:0 for rid in all_recipe_ids}

# helper to randomize a timestamp near last_ts or fresh
def next_timestamp_for_user(uid):
    base = user_last_ts.get(uid)
    if base is None:
        # start somewhere in last DAYS_SPAN days at a random time
        base = datetime.utcnow() - timedelta(days=random.randint(0, DAYS_SPAN), hours=random.randint(0,23), minutes=random.randint(0,59))
    # increment by a few seconds to minutes to simulate session steps
    delta_seconds = random.randint(5, 900)  # 5s to 15min between actions
    newt = base + timedelta(seconds=delta_seconds)
    # don't go into future
    if newt > datetime.utcnow():
        newt = datetime.utcnow() - timedelta(seconds=random.randint(10,300))
    user_last_ts[uid] = newt
    return newt

# Prebuild a simple user -> persona assignment to add consistency
user_persona_map = {}
for uid in user_ids:
    user_persona_map[uid] = random.choice(persona_keys)

# Main loop: produce events until we hit TARGET_INTERACTIONS
while events_created < TARGET_INTERACTIONS:
    # choose user randomly (weights could be added here for heavy users)
    user = random.choice(user_ids)
    persona = user_persona_map.get(user, random.choice(persona_keys))
    p = USER_PERSONAS[persona]

    # Decide whether to start a new session for this user
    cur_sess = user_session.get(user)
    if cur_sess is None or user_session_count.get(user,0) <= 0:
        # create new session
        sess_id = f"sess-{uuid.uuid4().hex[:10]}"
        user_session[user] = sess_id
        # how many events this session will have
        user_session_count[user] = random.randint(*p["session_events"])
        # small chance of a long session for viral scroller
        if persona == "viral_scroller" and random.random() < 0.15:
            user_session_count[user] += random.randint(10, 40)
    else:
        sess_id = cur_sess

    # choose recipe: trending bias + small chance to revisit user's recent recipe
    if random.random() < p.get("trend_bias", 0.08):
        rid = random.choice(trending_recipes)
    else:
        # 20% chance to pick a recipe the user recently visited
        if random.random() < 0.20 and "recent_recipe" in user_session:
            # sometimes pick the same recipe as the user's last event
            rid = user_session.get("recent_recipe", random.choice(all_recipe_ids))
        else:
            rid = random.choice(all_recipe_ids)

    # choose interaction type weighted by persona
    itype = weighted_choice(p["weights"])

    # Build metadata & description
    ts = next_timestamp_for_user(user)
    iid = f"int-{uuid.uuid4().hex[:10]}"

    # ensure recipe_title present (fetch once and cache is optional)
    recipe_doc = db.collection("recipes").document(rid).get().to_dict()
    recipe_title = recipe_doc.get("title", "Recipe")

    interaction = {
        "id": iid,
        "user_id": user,
        "recipe_id": rid,
        "type": itype,
        "timestamp": ts,
        "session_id": sess_id,
        "metadata": {}
    }

    # common platform/device/referrer
    platform_choices = ["android", "ios", "web"]
    device_by_platform = {
        "android": "mobile",
        "ios": "mobile",
        "web": random.choice(["desktop", "tablet"])
    }
    platform = random.choice(platform_choices)
    device = device_by_platform[platform]
    referrer = random.choices(["search", "homepage", "suggested", "social", "internal"], weights=[0.4,0.25,0.15,0.12,0.08], k=1)[0]

    if itype == "view":
        interaction["metadata"] = {
            "device": device,
            "platform": platform,
            "referrer": referrer,
            "duration_seconds": random.randint(4, 90),
            "scroll_depth": round(random.uniform(0.2, 0.99), 2)
        }
        interaction["description"] = f"{recipe_title} viewed by {user} on {platform}"

    elif itype == "like":
        interaction["metadata"] = {
            "device": device,
            "platform": platform,
            "referrer": referrer
        }
        interaction["description"] = f"{user} liked {recipe_title}"

    elif itype == "rating":
        # rating distribution depends on persona slightly
        if persona == "critic":
            rating_value = random.randint(1, 5)
        elif persona == "home_cook":
            rating_value = random.randint(3, 5)
        else:
            rating_value = random.randint(2, 5)
        interaction["metadata"] = {
            "rating": rating_value,
            "platform": platform,
            "device": device,
            "referrer": referrer
        }
        # add sentiment quick tag
        sentiment = "positive" if rating_value >= 4 else ("neutral" if rating_value == 3 else "negative")
        interaction["metadata"]["sentiment"] = sentiment
        interaction["description"] = f"{user} rated {recipe_title} {rating_value} stars"

    elif itype == "attempt":
        success = random.random() < 0.7 if persona == "home_cook" else random.random() < 0.45
        time_taken = random.randint(10, 180)
        notes = ""
        if random.random() < 0.12:
            notes = random.choice([
                "Added extra spices", "Reduced salt", "Used ghee instead of oil", "Took longer than expected"
            ])
        interaction["metadata"] = {
            "success": success,
            "time_taken_minutes": time_taken,
            "platform": platform,
            "device": device,
            "referrer": referrer,
            "notes": notes
        }
        status = "success" if success else "failed"
        interaction["description"] = f"{user} attempted {recipe_title} ({status})"

    # push to Firestore
    db.collection("interactions").document(iid).set(interaction)

    # bookkeeping
    events_created += 1
    counts_by_type[itype] = counts_by_type.get(itype, 0) + 1
    counts_by_recipe[rid] = counts_by_recipe.get(rid, 0) + 1
    user_session_count[user] = user_session_count.get(user, 1) - 1
    # store last recipe for possible revisit behavior
    # (we store in user_session under a reserved key name)
    user_session["recent_recipe"] = rid

# done
print(f"✔ Done: generated {events_created} interactions.")
print("Type counts:", counts_by_type)

# show top 5 recipes by interactions
top_recipes = sorted(counts_by_recipe.items(), key=lambda x: x[1], reverse=True)[:8]
print("Top recipe activity (top 8):")
for rid, cnt in top_recipes:
    try:
        title = db.collection("recipes").document(rid).get().to_dict().get("title", rid)
    except Exception:
        title = rid
    print(f" - {title} ({rid}): {cnt} interactions")

print("Trending recipes were:", trending_recipes)

# ----------------------------------------------------------
# 5. BASELINE BALANCED INTERACTIONS FOR ALL RECIPES
# Ensures EVERY recipe has views + likes + some ratings
# ----------------------------------------------------------

print("\nAdding BASELINE interactions for clean analytics...")

for rid in all_recipe_ids:
    recipe_doc = db.collection("recipes").document(rid).get().to_dict()
    recipe_title = recipe_doc.get("title", "Recipe")

    # 10–25 guaranteed views
    view_count = random.randint(10, 25)

    # 3–10 likes (must be <= views)
    like_count = random.randint(3, min(10, view_count))

    # 1–5 ratings
    rating_count = random.randint(1, 5)

    # 0–3 attempts
    attempt_count = random.randint(0, 3)

    # Random user for each event
    for _ in range(view_count):
        uid = random.choice(user_ids)
        iid = f"int-{uuid.uuid4().hex[:10]}"
        db.collection("interactions").document(iid).set({
            "id": iid,
            "user_id": uid,
            "recipe_id": rid,
            "type": "view",
            "timestamp": datetime.utcnow() - timedelta(days=random.randint(0, 120)),
            "metadata": {"device": random.choice(["mobile", "desktop"])},
            "description": f"{uid} viewed {recipe_title}"
        })

    for _ in range(like_count):
        uid = random.choice(user_ids)
        iid = f"int-{uuid.uuid4().hex[:10]}"
        db.collection("interactions").document(iid).set({
            "id": iid,
            "user_id": uid,
            "recipe_id": rid,
            "type": "like",
            "timestamp": datetime.utcnow() - timedelta(days=random.randint(0, 120)),
            "metadata": {},
            "description": f"{uid} liked {recipe_title}"
        })

    for _ in range(rating_count):
        uid = random.choice(user_ids)
        stars = random.randint(3, 5)
        iid = f"int-{uuid.uuid4().hex[:10]}"
        db.collection("interactions").document(iid).set({
            "id": iid,
            "user_id": uid,
            "recipe_id": rid,
            "type": "rating",
            "timestamp": datetime.utcnow() - timedelta(days=random.randint(0, 120)),
            "metadata": {"rating": stars},
            "description": f"{uid} rated {recipe_title} {stars} stars"
        })

    for _ in range(attempt_count):
        uid = random.choice(user_ids)
        success = random.choice([True, False])
        iid = f"int-{uuid.uuid4().hex[:10]}"
        db.collection("interactions").document(iid).set({
            "id": iid,
            "user_id": uid,
            "recipe_id": rid,
            "type": "attempt",
            "timestamp": datetime.utcnow() - timedelta(days=random.randint(0, 120)),
            "metadata": {
                "success": success,
                "time_taken_minutes": random.randint(20, 120),
            },
            "description": f"{uid} attempted {recipe_title} ({'success' if success else 'failed'})"
        })

print("✔ Baseline interactions added.")
