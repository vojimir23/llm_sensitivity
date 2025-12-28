import psycopg2
from psycopg2 import sql

class DataStore:
    def __init__(self, db_config):
        self.conn = psycopg2.connect(**db_config)
        self.cursor = self.conn.cursor()

    def token_exists(self, token_number):
        """ Check if the token number exists in the database. """
        query = "SELECT COUNT(*) FROM original_sentences WHERE token_number = %s"
        self.cursor.execute(query, (token_number,))
        count = self.cursor.fetchone()[0]
        return count > 0       

    def fetch_expressions(self):
        query = "SELECT expression_id, expression FROM expressions where expression_sensitivity_class='Taboo'"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def insert_original_sentences(self, sentences):
        """ Insert sentences into the database after checking for existing token numbers. """
        insert_query = """
        INSERT INTO original_sentences (expression_id, original_sentence, token_number, corpus_name, sources,
                                        website, title, crawl_date, url, topic, genre)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for sentence in sentences:
            if not self.token_exists(sentence[2]):  # Check token number (index 2 in tuple)
                self.cursor.execute(insert_query, sentence)
        self.conn.commit()


##take original sentences to be paraphrased
    def fetch_original_sentences(self):
        query = """
                   SELECT original_sentence_id, original_sentence
                    from original_sentences  as os 
                    join expressions as ex on ex.expression_id=os.expression_id where ex.expression_sensitivity_class='Informal' Offset 15000

        """
        self.cursor.execute(query)
        return self.cursor.fetchall()
    ####classifiying by mistral from server
    def insert_classification_response_mistral(self, original_sentence_id, original_classification):
        try:
            self.cursor.execute("""
                INSERT INTO classification_mistral (original_sentence_id, original_sentence_sensitivity_class) 
                VALUES (%s, %s);
            """, (original_sentence_id, original_classification))
            self.conn.commit()
        except Exception as e:
            print(f"Failed to insert new classification row: {e}")
            self.conn.rollback()

    def fetch_for_llm(self):

        query = """
        SELECT ex."key", os.original_sentence AS sentence
        FROM original_sentences AS os
        JOIN experts_classification AS ex
          ON ex."key" = os.original_sentence_id

        UNION

        SELECT ex."key", ps.paraphrased_sentence AS sentence
        FROM paraphrased_sentences AS ps
        JOIN experts_classification AS ex
          ON ex."key" = ps.paraphrased_sentence_id;

        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def insert_llm_classification(self, key, classification, model, model_system_fingerprint, comments=""):

        query = """
        INSERT INTO llm_classification (key, classification, comments, model, model_system_fingerprint)
        VALUES (%s, %s, %s, %s, %s)
        """
        self.cursor.execute(query, (key, classification, comments, model, model_system_fingerprint))
        self.conn.commit()


    def close(self):
        self.cursor.close()
        self.conn.close()

