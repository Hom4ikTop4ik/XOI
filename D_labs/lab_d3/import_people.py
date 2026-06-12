import sys
import threading
import queue
import time
import os
import psycopg2
from psycopg2.extras import execute_values
import xml.sax
from xml.sax.handler import ContentHandler
from typing import List, Tuple, Optional
from dataclasses import dataclass
from contextlib import contextmanager


@dataclass
class Person:
    """Raw person data from XML"""
    id: str
    firstname: str
    surname: str
    gender: str


@dataclass
class Relation:
    """Raw relation data from XML"""
    person_id: str
    relation_type: str
    ref_id: str


class ChunkedXMLReader:
    def __init__(self, filename: str, num_chunks: int):
        self.filename = filename
        self.num_chunks = num_chunks
        self.file_size = os.path.getsize(filename)
        
    def find_chunk_boundaries(self) -> List[Tuple[int, int]]:
        """ Поиск границ чанка """
        chunk_size = self.file_size // self.num_chunks
        boundaries = []
        
        with open(self.filename, 'rb') as f:
            for i in range(self.num_chunks):
                start = i * chunk_size
                if i > 0:
                    # Adjust start to beginning of next person element
                    start = self._adjust_to_person_start(start)
                
                end = (i + 1) * chunk_size if i < self.num_chunks - 1 else self.file_size
                if i < self.num_chunks - 1:
                    end = self._adjust_to_person_end(end)
                
                boundaries.append((start, end))
        
        return boundaries
    
    def _adjust_to_person_start(self, position: int) -> int:
        """ Переместить "курсор" к началу тега person """
        with open(self.filename, 'rb') as f:
            f.seek(position)
            # Read forward until we find '<person'
            data = f.read(10000)
            pos = data.find(b'<person')
            if pos != -1:
                return position + pos
            # If not found, scan further
            while pos == -1:
                data = f.read(10000)
                if not data:
                    break
                pos = data.find(b'<person')
                if pos != -1:
                    return f.tell() - len(data) + pos
        return position
    
    def _adjust_to_person_end(self, position: int) -> int:
        """ Переместить "курсор" к концу закрывающего тега /person """
        with open(self.filename, 'rb') as f:
            if position >= self.file_size:
                return self.file_size
            
            f.seek(max(0, position - 10000))
            data = f.read(min(20000, self.file_size - f.tell()))
            
            # Find last </person> before position
            last_person_end = data.rfind(b'</person>')
            if last_person_end != -1:
                return f.tell() - len(data) + last_person_end + len(b'</person>')
        
        return position


class PersonHandler(ContentHandler):
    """ SAX handler for extracting person and relation data """

    def __init__(self, person_queue: queue.Queue, relation_queue: queue.Queue):
        super().__init__()
        self.person_queue = person_queue
        self.relation_queue = relation_queue
        self.current_person: Optional[Person] = None
        self.current_element = ""
        self.current_text = ""
        
    def startElement(self, name: str, attrs):
        self.current_element = name
        self.current_text = ""
        
        if name == 'person':
            person_id = attrs.getValue('id')
            self.current_person = Person(
                id=person_id,
                firstname='',
                surname='',
                gender=''
            )
            
        elif name in ['parent', 'father', 'mother',
                      'spouse', 'husband', 'wife',
                      'child', 'son', 'daughter',
                      'sibling', 'brother', 'sister']:
            if self.current_person:
                ref = attrs.getValue('ref')
                if ref:
                    self.relation_queue.put(Relation(
                        person_id=self.current_person.id,
                        relation_type=name,
                        ref_id=ref
                    ))
                    
    def characters(self, content: str):
        if self.current_element in ['firstname', 'surname', 'gender']:
            self.current_text += content.strip()
            
    def endElement(self, name: str):
        if name == 'person' and self.current_person:
            self.person_queue.put(self.current_person)
            self.current_person = None
        elif name in ['firstname', 'surname', 'gender'] and self.current_person:
            if name == 'firstname':
                self.current_person.firstname = self.current_text
            elif name == 'surname':
                self.current_person.surname = self.current_text
            elif name == 'gender':
                self.current_person.gender = self.current_text
        self.current_element = ""


class StreamingImporter:
    """Handles parallel streaming import into raw tables"""
    
    CREATE_RAW_TABLES_SQL = """
    DROP TABLE IF EXISTS raw_person CASCADE;
    CREATE TABLE raw_person (
        id VARCHAR(7) PRIMARY KEY,
        firstname VARCHAR(100),
        surname VARCHAR(100),
        gender VARCHAR(6)
    );
    
    DROP TABLE IF EXISTS raw_relation CASCADE;
    CREATE TABLE raw_relation (
        person_id VARCHAR(7),   
        relation_type VARCHAR(20),
        ref_id VARCHAR(7),
        INDEX idx_raw_relation_person (person_id),
        INDEX idx_raw_relation_ref (ref_id)
    );
    """
    
    def __init__(self, db_url: str, num_threads: int):
        self.db_url = db_url
        self.num_threads = num_threads
        self.person_queue = queue.Queue(maxsize=10000)
        self.relation_queue = queue.Queue(maxsize=10000)
        
    def init_raw_schema(self):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(self.CREATE_RAW_TABLES_SQL)
            conn.commit()
            
    @contextmanager
    def get_connection(self):
        """Get database connection with autocommit off"""
        conn = psycopg2.connect(self.db_url)
        try:
            yield conn
        finally:
            conn.close()
            
    def worker_insert_persons(self, worker_id: int):
        """Worker thread for batch inserting persons"""
        batch = []
        batch_size = 1000
        
        while True:
            try:
                person = self.person_queue.get(timeout=1)
                batch.append((person.id, person.firstname, person.surname, person.gender))
                
                if len(batch) >= batch_size:
                    self._flush_persons(batch)
                    batch = []
                    
            except queue.Empty:
                if batch:
                    self._flush_persons(batch)
                break
                
    def _flush_persons(self, batch: List[Tuple]):
        """Flush batch of persons to database"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    "INSERT INTO raw_person (id, firstname, surname, gender) VALUES %s",
                    batch
                )
            conn.commit()
            
    def worker_insert_relations(self, worker_id: int):
        """Worker thread for batch inserting relations"""
        batch = []
        batch_size = 5000
        
        while True:
            try:
                relation = self.relation_queue.get(timeout=1)
                batch.append((relation.person_id, relation.relation_type, relation.ref_id))
                
                if len(batch) >= batch_size:
                    self._flush_relations(batch)
                    batch = []
                    
            except queue.Empty:
                if batch:
                    self._flush_relations(batch)
                break
                
    def _flush_relations(self, batch: List[Tuple]):
        """Flush batch of relations to database"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    "INSERT INTO raw_relation (person_id, relation_type, ref_id) VALUES %s",
                    batch
                )
            conn.commit()
            
    def parse_chunk(self, start_pos: int, end_pos: int, thread_id: int):
        """Parse a chunk of XML file using SAX"""
        with open('output.xml', 'rb') as f:
            f.seek(start_pos)
            # Чтение чанка
            chunk_size = end_pos - start_pos
            data = f.read(chunk_size)
            
            # Смещение к началу полезных данных
            if start_pos > 0:
                data = data[data.find(b'<person'):]
            
            # Parse with SAX
            handler = PersonHandler(self.person_queue, self.relation_queue)
            parser = xml.sax.make_parser()
            parser.setContentHandler(handler)
            
            # Parse the chunk (may need to be wrapped in a root element)
            try:
                # For partial XML, wrap in a fake root
                wrapped_data = b'<root>' + data + b'</root>'
                parser.parse(wrapped_data)
            except Exception as e:
                print(f"Thread {thread_id} parse error: {e}, continuing...")
                
    def run_streaming_phase(self, xml_file: str):
        """Main streaming phase with parallel chunk processing"""
        print("Starting streaming phase...")
        start_time = time.time()
        
        self.init_raw_schema()
        
        chunker = ChunkedXMLReader(xml_file, self.num_threads)
        boundaries = chunker.find_chunk_boundaries()
        
        # Запуск работяг для вставок в БД
        person_threads = []
        relation_threads = []
        
        for i in range(self.num_threads):
            t = threading.Thread(target=self.worker_insert_persons, args=(i,))
            t.start()
            person_threads.append(t)
            
        for i in range(self.num_threads):
            t = threading.Thread(target=self.worker_insert_relations, args=(i,))
            t.start()
            relation_threads.append(t)
            
        # Работяги-парсеры (parsing threads)
        parse_threads = []
        for i, (start, end) in enumerate(boundaries):
            t = threading.Thread(target=self.parse_chunk, args=(start, end, i))
            t.start()
            parse_threads.append(t)

        # Wait for parsing to complete
        for t in parse_threads:
            t.join()

        # "Работяги, конец смены!" (Signal workers to finish)
        for _ in range(self.num_threads):
            self.person_queue.put(None)
            self.relation_queue.put(None)
            
        # Wait for workers to finish
        for t in person_threads:
            t.join()
        for t in relation_threads:
            t.join()
            
        elapsed = time.time() - start_time
        print(f"Streaming phase completed in {elapsed} seconds")


class Normalizer:
    def __init__(self, db_url: str):
        self.db_url = db_url
        
    def create_target_schema(self):
        """ Создание строгих таблиц """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Create person table
                cur.execute("""
                    DROP TABLE IF EXISTS person CASCADE;
                    CREATE TABLE person (
                        id VARCHAR(7) PRIMARY KEY,
                        firstname VARCHAR(100) NOT NULL,
                        surname VARCHAR(100) NOT NULL,
                        gender BOOLEAN NOT NULL,
                        CONSTRAINT chk_person_id_format CHECK (id ~ '^P[0-9]{6}$')
                    );
                """)
                
                # Create parent_of table
                cur.execute("""
                    DROP TABLE IF EXISTS parent_of CASCADE;
                    CREATE TABLE parent_of (
                        parent_id VARCHAR(7) NOT NULL,
                        child_id VARCHAR(7) NOT NULL,
                        PRIMARY KEY (parent_id, child_id),
                        FOREIGN KEY (parent_id) REFERENCES person(id) ON DELETE CASCADE,
                        FOREIGN KEY (child_id) REFERENCES person(id) ON DELETE CASCADE,
                        CONSTRAINT chk_parent_not_self CHECK (parent_id != child_id)
                    );
                """)
                
                # Create spouse_of table
                cur.execute("""
                    DROP TABLE IF EXISTS spouse_of CASCADE;
                    CREATE TABLE spouse_of (
                        person1_id VARCHAR(7) NOT NULL,
                        person2_id VARCHAR(7) NOT NULL,
                        PRIMARY KEY (person1_id, person2_id),
                        FOREIGN KEY (person1_id) REFERENCES person(id) ON DELETE CASCADE,
                        FOREIGN KEY (person2_id) REFERENCES person(id) ON DELETE CASCADE,
                        CONSTRAINT chk_spouse_not_self CHECK (person1_id != person2_id),
                        CONSTRAINT uk_spouse_person1 UNIQUE (person1_id),
                        CONSTRAINT uk_spouse_person2 UNIQUE (person2_id),
                        CONSTRAINT chk_spouse_order CHECK (person1_id < person2_id)
                    );
                """)
                
                # Create sibling_of table
                cur.execute("""
                    DROP TABLE IF EXISTS sibling_of CASCADE;
                    CREATE TABLE sibling_of (
                        person1_id VARCHAR(7) NOT NULL,
                        person2_id VARCHAR(7) NOT NULL,
                        PRIMARY KEY (person1_id, person2_id),
                        FOREIGN KEY (person1_id) REFERENCES person(id) ON DELETE CASCADE,
                        FOREIGN KEY (person2_id) REFERENCES person(id) ON DELETE CASCADE,
                        CONSTRAINT chk_sibling_not_self CHECK (person1_id != person2_id),
                        CONSTRAINT chk_sibling_order CHECK (person1_id < person2_id)
                    );
                """)
                
                # Create indexes
                cur.execute("CREATE INDEX idx_parent_of_parent ON parent_of(parent_id);")
                cur.execute("CREATE INDEX idx_parent_of_child ON parent_of(child_id);")
                cur.execute("CREATE INDEX idx_spouse_of_person2 ON spouse_of(person2_id);")
                cur.execute("CREATE INDEX idx_sibling_of_person2 ON sibling_of(person2_id);")
                
                # Create gender check function and trigger
                cur.execute("""
                    CREATE OR REPLACE FUNCTION check_spouse_gender_different()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        IF EXISTS (
                            SELECT 1 FROM person p1, person p2
                            WHERE p1.id = NEW.person1_id 
                              AND p2.id = NEW.person2_id
                              AND p1.gender = p2.gender
                        ) THEN
                            RAISE EXCEPTION 'Супруги должны быть разного пола';
                        END IF;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                    
                    DROP TRIGGER IF EXISTS trg_spouse_gender_check ON spouse_of;
                    CREATE TRIGGER trg_spouse_gender_check
                        BEFORE INSERT OR UPDATE ON spouse_of
                        FOR EACH ROW
                        EXECUTE FUNCTION check_spouse_gender_different();
                """)
                
            conn.commit()
            
    def normalize_persons(self):
        """ Вставка из raw_person с проверкой на полноту данных """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Insert only persons with complete data (no empty strings AND with gender)
                cur.execute("""
                    INSERT INTO person (id, firstname, surname, gender)
                    SELECT 
                        id,
                        NULLIF(firstname, ''),
                        NULLIF(surname, ''),
                        CASE 
                            WHEN gender = 'male' THEN TRUE
                            WHEN gender = 'female' THEN FALSE
                        END as gender
                    FROM raw_person
                    WHERE firstname IS NOT NULL 
                      AND firstname != ''
                      AND surname IS NOT NULL 
                      AND surname != ''
                      AND gender IN ('male', 'female')
                """)
                
                count = cur.rowcount
                conn.commit()
                print(f"Normalized {count} persons")
                return count
                
    def normalize_parent_relations(self):
        """Normalize parent relations (parent, father, mother -> parent_of)"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Add temporary index for performance
                cur.execute("CREATE INDEX IF NOT EXISTS tmp_raw_relation_person_ref ON raw_relation(person_id, ref_id);")
                
                # Insert parent relations (parent, father, mother)
                cur.execute("""
                    INSERT INTO parent_of (parent_id, child_id)
                    SELECT DISTINCT r.ref_id, r.person_id
                    FROM raw_relation r
                    WHERE r.relation_type IN ('parent', 'father', 'mother')
                      AND EXISTS (SELECT 1 FROM person p WHERE p.id = r.ref_id)
                      AND EXISTS (SELECT 1 FROM person p WHERE p.id = r.person_id)
                      AND r.ref_id != r.person_id
                    ON CONFLICT DO NOTHING
                """)
                
                count = cur.rowcount
                conn.commit()
                print(f"Normalized {count} parent relations")
                return count
                
    def normalize_spouse_relations(self):
        """Normalize spouse relations (spouse, husband, wife -> spouse_of)"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Insert spouse relations with ordering (person1_id < person2_id)
                cur.execute("""
                    INSERT INTO spouse_of (person1_id, person2_id)
                    SELECT DISTINCT
                        LEAST(r.person_id, r.ref_id),
                        GREATEST(r.person_id, r.ref_id)
                    FROM raw_relation r
                    WHERE r.relation_type IN ('spouse', 'husband', 'wife')
                      AND EXISTS (SELECT 1 FROM person p WHERE p.id = r.person_id)
                      AND EXISTS (SELECT 1 FROM person p WHERE p.id = r.ref_id)
                      AND r.person_id != r.ref_id
                    ON CONFLICT DO NOTHING
                """)
                
                count = cur.rowcount
                conn.commit()
                print(f"Normalized {count} spouse relations")
                return count
                
    def normalize_sibling_relations(self):
        """Normalize sibling relations (sibling, brother, sister -> sibling_of)"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Insert sibling relations with ordering
                cur.execute("""
                    INSERT INTO sibling_of (person1_id, person2_id)
                    SELECT DISTINCT
                        LEAST(r.person_id, r.ref_id),
                        GREATEST(r.person_id, r.ref_id)
                    FROM raw_relation r
                    WHERE r.relation_type IN ('sibling', 'brother', 'sister')
                      AND EXISTS (SELECT 1 FROM person p WHERE p.id = r.person_id)
                      AND EXISTS (SELECT 1 FROM person p WHERE p.id = r.ref_id)
                      AND r.person_id != r.ref_id
                    ON CONFLICT DO NOTHING
                """)
                
                count = cur.rowcount
                conn.commit()
                print(f"Normalized {count} sibling relations")
                return count
                
    def validate_data(self):
        """Run validation queries to check data integrity"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                print("\n=== Data Validation ===")
                
                # Check persons count
                cur.execute("SELECT COUNT(*) FROM person")
                persons = cur.fetchone()[0]
                print(f"Total persons: {persons}")
                
                # Check relations counts
                cur.execute("SELECT COUNT(*) FROM parent_of")
                print(f"Parent relations: {cur.fetchone()[0]}")
                
                cur.execute("SELECT COUNT(*) FROM spouse_of")
                print(f"Spouse relations: {cur.fetchone()[0]}")
                
                cur.execute("SELECT COUNT(*) FROM sibling_of")
                print(f"Sibling relations: {cur.fetchone()[0]}")
                
                # Check for missing required data in raw
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_raw,
                        SUM(CASE WHEN firstname = '' OR surname = '' THEN 1 ELSE 0 END) as missing_names,
                        SUM(CASE WHEN gender NOT IN ('male', 'female') THEN 1 ELSE 0 END) as invalid_gender
                    FROM raw_person
                """)
                total, missing_names, invalid_gender = cur.fetchone()
                print(f"\nRaw data stats: {total} total, {missing_names} missing names, {invalid_gender} invalid gender")
                
    @contextmanager
    def get_connection(self):
        conn = psycopg2.connect(self.db_url)
        try:
            yield conn
        finally:
            conn.close()


def main():
    if len(sys.argv) != 4:
        print("Usage: python import_people.py <xml_file> <db_url> <num_threads>")
        print("Example: python import_people.py output.xml postgresql://user:pass@localhost/db 4")
        sys.exit(1)
        
    xml_file = sys.argv[1]
    db_url = sys.argv[2]
    num_threads = int(sys.argv[3])
    
    # 1. Импорт xml
    print(f"Importing `{xml_file}` into `{db_url}` with `{num_threads}` threads")
    start_1 = time.time()
    importer = StreamingImporter(db_url, num_threads)
    importer.run_streaming_phase(xml_file)

    # 2. Нормализация данных
    normalizer = Normalizer(db_url)
    normalizer.create_target_schema()
    print("\nStarting normalization phase...")
    start_2 = time.time()
    # Add indexes on raw tables for faster normalization
    normalizer.normalize_persons()
    normalizer.normalize_parent_relations()
    normalizer.normalize_spouse_relations()
    normalizer.normalize_sibling_relations()
    
    # Валидация данных
    print("\nStarting validation phase...")
    start_3 = time.time()
    normalizer.validate_data()

    print(f"  Times:\n    import: {start_2 - start_1}secs\n    normalization: {start_3 - start_2}secs\n    validation: {time.time() - start_3} secs\n")
    
    print("\nImport completed successfully!")


if __name__ == "__main__":
    main()
