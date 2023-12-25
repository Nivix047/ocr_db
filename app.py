import os
import io
import PyPDF2
import psycopg2
from psycopg2 import sql

def extract_text_from_pdf(pdf_path):
    try:
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            text = ""
            for page in pdf_reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text
            if not text:
                raise ValueError("No text was extracted from the PDF.")
            return text
    except Exception as e:
        raise Exception(f"An error occurred while extracting text: {str(e)}")

def insert_into_db(content, conn):
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO pdf_data (content) VALUES (%s)", (content,))
            conn.commit()
    except Exception as e:
        raise Exception(f"Error inserting into database: {str(e)}")

def main():
    # Replace with your PostgreSQL connection details
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"), 
        user=os.getenv("DB_USER"), 
        password=os.getenv("DB_PASSWORD"), 
        host=os.getenv("DB_HOST")
    )
    
    # Replace with the path to your PDF file
    pdf_path = "/Users/nivix047/Desktop/mongoCRUD.pdf"

    try:
        text = extract_text_from_pdf(pdf_path)
        insert_into_db(text, conn)
        print("PDF content successfully stored in the database.")
    except Exception as e:
        print(e)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
