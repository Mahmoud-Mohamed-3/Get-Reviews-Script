from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import psycopg2
import time
import logging
import random

DB_HOST = "localhost"
DB_NAME = "talabat_reviews"
DB_USER = "postgres"
DB_PASSWORD = "123456"

service = Service(executable_path='/usr/bin/chromedriver')
chrome_options = Options()
chrome_options.binary_location = '/usr/bin/chromium-browser'
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")

def connect_to_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Connected to the database")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

conn = connect_to_db()

def insert_review(conn, review_text, rating, author_name, review_source, restaurant_name):
    try:
        cur = conn.cursor()
        # Check for duplicates
        cur.execute(
            "SELECT * FROM reviews WHERE review_text = %s AND author_name = %s AND restaurant_name = %s",
            (review_text, author_name, restaurant_name)
        )
        if cur.fetchone() is None:  # Insert only if no duplicate exists
            cur.execute(
                "INSERT INTO reviews (review_text, rating, author_name, review_source, restaurant_name) VALUES (%s, %s, %s, %s, %s)",
                (review_text, rating, author_name, review_source, restaurant_name)
            )
            conn.commit()
        cur.close()
    except Exception as e:
        print(f"Error inserting review: {e}")

try:
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(f'https://www.talabat.com/egypt/restaurants/')
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, "html.parser")
    total_number_of_pages = soup.find("ul", attrs={"data-test": "pagination"}).find_all("li")[-2].find("a").text

    for i in range(1, 3):
        driver.get(f'https://www.talabat.com/egypt/restaurants/?page={i}')
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        total_cards_in_page = soup.find_all("div", attrs={"data-testid": "vendor"})
        for card in total_cards_in_page:
            restaurant_page_link = card.find("a")["href"]
            restaurant_name = card.find("p", attrs={"data-testid": "vendor-name"}).text

            try:
                driver.get(f'https://www.talabat.com{restaurant_page_link}')
                review_page_source = driver.page_source
                soup = BeautifulSoup(review_page_source, "html.parser")
                wait = WebDriverWait(driver, 10)

                def click_read_more():
                    try:
                        read_more_button = wait.until(
                            EC.element_to_be_clickable((By.XPATH, '//button[@data-testid="btn-load-more"]'))
                        )
                        read_more_button.click()
                        return True
                    except:
                        return False

                def extract_reviews():
                    wait.until(EC.presence_of_element_located((By.XPATH, '//div[@data-testid="reviews-item-component"]')))
                    page_source = driver.page_source
                    soup = BeautifulSoup(page_source, "html.parser")
                    reviews = soup.find_all("div", attrs={"data-testid": "reviews-item-component"})
                    if not reviews:
                        raise Exception("No reviews found on the page.")
                    else:
                        print("Extracted reviews")
                        return reviews

                if not conn:
                    raise Exception("Failed to connect to the database.")

                all_reviews = []
                seen_reviews = set()  # Track seen reviews to avoid duplicates
                previous_review_count = 0

                while True:
                    reviews = extract_reviews()
                    if len(reviews) == previous_review_count:
                        break  # Stop if no new reviews are loaded
                    previous_review_count = len(reviews)

                    for review in reviews:
                        review_text = review.find("p", attrs={"data-testid": "customer-review"}).text
                        if review_text not in seen_reviews:
                            seen_reviews.add(review_text)
                            all_reviews.append(review)

                    if not click_read_more():
                        break
                    time.sleep(2)

                for review in all_reviews:
                    try:
                        rating = review.find("div", attrs={"data-testid": "restaurant-rating-comp"}).find("div").find("div").text
                        author_name = review.find("div", attrs={"data-testid": "customer-name"}).text
                        text = review.find("p", attrs={"data-testid": "customer-review"}).text
                        review_source = "Talabat"
                        insert_review(conn, text, rating, author_name, review_source, restaurant_name)
                    except Exception as e:
                        logging.error(f"Error processing review: {e}")
                        time.sleep(random.uniform(2, 5))

            except Exception as e:
                print(f"Error processing restaurant page: {e}")
                time.sleep(random.uniform(2, 5))

        time.sleep(random.uniform(2, 5))

except Exception as e:
    print(f"Error initializing the driver: {e}")
    if 'driver' in locals():
        driver.quit()

finally:
    if conn:
        conn.close()
        print("Connection closed")
    if 'driver' in locals():
        driver.quit()
        print("Driver closed")