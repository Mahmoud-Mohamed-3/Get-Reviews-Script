import time
import random
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import psycopg2

# Database configuration
DB_HOST = "localhost"
DB_NAME = "talabat_reviews"
DB_USER = "postgres"
DB_PASSWORD = "123456"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# WebDriver setup
service = Service(executable_path='/usr/bin/chromedriver')
chrome_options = Options()
chrome_options.binary_location = '/usr/bin/chromium-browser'
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--headless")  # Run in headless mode to save resources
chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

def connect_to_db():
    """Connect to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        logging.info("Connected to the database")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        return None

def insert_review(conn, review_text, rating, author_name, review_source, restaurant_name):
    """Insert a review into the database, avoiding duplicates."""
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
            logging.info(f"Inserted review for {restaurant_name} by {author_name}")
        cur.close()
    except Exception as e:
        logging.error(f"Error inserting review: {e}")

def click_read_more(driver):
    """Click the 'Read More' button to load additional reviews."""
    try:
        read_more_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//button[@data-testid="btn-load-more"]'))
        )
        read_more_button.click()
        return True
    except:
        return False

def extract_reviews(driver):
    """Extract reviews from the current page."""
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//div[@data-testid="reviews-item-component"]'))
        )
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        reviews = soup.find_all("div", attrs={"data-testid": "reviews-item-component"})
        if not reviews:
            raise Exception("No reviews found on the page.")
        return reviews
    except Exception as e:
        logging.error(f"Error extracting reviews: {e}")
        return []

def process_restaurant(driver, conn, restaurant_page_link, restaurant_name):
    """Process reviews for a single restaurant."""
    try:
        driver.get(f'https://www.talabat.com{restaurant_page_link}')
        logging.info(f"Processing restaurant: {restaurant_name}")

        all_reviews = []
        seen_reviews = set()  # Track seen reviews to avoid duplicates
        previous_review_count = 0

        while True:
            reviews = extract_reviews(driver)
            if len(reviews) == previous_review_count:
                break  # Stop if no new reviews are loaded
            previous_review_count = len(reviews)

            for review in reviews:
                review_text = review.find("p", attrs={"data-testid": "customer-review"}).text
                if review_text not in seen_reviews:
                    seen_reviews.add(review_text)
                    all_reviews.append(review)

            if not click_read_more(driver):
                break
            time.sleep(random.uniform(2, 5))  # Add random delays

        for review in all_reviews:
            try:
                rating = review.find("div", attrs={"data-testid": "restaurant-rating-comp"}).find("div").find("div").text
                author_name = review.find("div", attrs={"data-testid": "customer-name"}).text
                text = review.find("p", attrs={"data-testid": "customer-review"}).text
                review_source = "Talabat"
                insert_review(conn, text, rating, author_name, review_source, restaurant_name)
            except Exception as e:
                logging.error(f"Error processing review: {e}")

    except Exception as e:
        logging.error(f"Error processing restaurant {restaurant_name}: {e}")

def main():
    """Main function to scrape reviews from Talabat."""
    conn = connect_to_db()
    if not conn:
        return

    try:
        # Initialize WebDriver
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(30)  # Set page load timeout to 30 seconds

        # Load the main restaurants page
        driver.get('https://www.talabat.com/uae/restaurants/')

        # Wait for restaurant cards to load
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, '//div[@data-testid="vendor"]'))
            )
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, "html.parser")
            total_cards_in_page = soup.find_all("div", attrs={"data-testid": "vendor"})
        except Exception as e:
            logging.error(f"Error waiting for restaurant cards to load: {e}")
            total_cards_in_page = []

        if not total_cards_in_page:
            logging.warning("No restaurant cards found on the page.")
            return

        # Process restaurant cards
        for i, card in enumerate(total_cards_in_page[:3]):  # Process only 3 restaurants
            try:
                restaurant_page_link = card.find("a")["href"]
                restaurant_name = card.find("p", attrs={"data-testid": "vendor-name"}).text
                process_restaurant(driver, conn, restaurant_page_link, restaurant_name)
                time.sleep(random.uniform(2, 5))  # Add random delays
            except Exception as e:
                logging.error(f"Error processing restaurant: {e}")
                continue

    except Exception as e:
        logging.error(f"Error in main function: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed")
        if driver:
            driver.quit()
            logging.info("WebDriver closed")

if __name__ == "__main__":
    main()