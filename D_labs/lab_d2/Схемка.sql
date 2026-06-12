-- -- schema.sql -- --

-- 1. Таблицы
-- Сам челик
CREATE TABLE person (
    id          VARCHAR(7) PRIMARY KEY,
    firstname   VARCHAR(100) NOT NULL,
    surname     VARCHAR(100) NOT NULL,
    gender      BOOLEAN NOT NULL,       -- TRUE = male, FALSE = female
    
    CONSTRAINT chk_person_id_format 
        CHECK (id ~ '^P[0-9]{6}$')
);


-- Таблица "родитель-ребёнок"
CREATE TABLE parent_of (
    parent_id   VARCHAR(7) NOT NULL,
    child_id    VARCHAR(7) NOT NULL,
    
    PRIMARY KEY (parent_id, child_id),
    FOREIGN KEY (parent_id) REFERENCES person(id) ON DELETE CASCADE,
    FOREIGN KEY (child_id)  REFERENCES person(id) ON DELETE CASCADE,
    
    -- Запрещаем петли (родитель != ребёнок)
    CONSTRAINT chk_parent_not_self CHECK (parent_id != child_id)
);

-- Таблица супругof
CREATE TABLE spouse_of (
    person1_id  VARCHAR(7) NOT NULL,
    person2_id  VARCHAR(7) NOT NULL,
    
    PRIMARY KEY (person1_id, person2_id),
    FOREIGN KEY (person1_id) REFERENCES person(id) ON DELETE CASCADE,
    FOREIGN KEY (person2_id) REFERENCES person(id) ON DELETE CASCADE,
    
    -- Запрещаем брак с самим собой
    CONSTRAINT chk_spouse_not_self CHECK (person1_id != person2_id),
    
    -- Уникальность: у человека не более одного супруга
    -- (обеспечивается через два уникальных индекса)
    CONSTRAINT uk_spouse_person1 UNIQUE (person1_id),
    CONSTRAINT uk_spouse_person2 UNIQUE (person2_id),
    
    -- Симметричность: храним только пару (меньший id, больший id)
    CONSTRAINT chk_spouse_order CHECK (person1_id < person2_id)
);

-- Таблица братья-сёстры
CREATE TABLE sibling_of (
    person1_id  VARCHAR(7) NOT NULL,
    person2_id  VARCHAR(7) NOT NULL,
    
    PRIMARY KEY (person1_id, person2_id),
    FOREIGN KEY (person1_id) REFERENCES person(id) ON DELETE CASCADE,
    FOREIGN KEY (person2_id) REFERENCES person(id) ON DELETE CASCADE,
    
    CONSTRAINT chk_sibling_not_self CHECK (person1_id != person2_id),
    CONSTRAINT chk_sibling_order CHECK (person1_id < person2_id)
);



-- 2. Дополнительная *православная* проверка пола супругов
-- (вроде нельзя в CHECK через подзапрос, поэтому через триггер BEFORE INSERT OR UPDATE)

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

CREATE TRIGGER trg_spouse_gender_check
    BEFORE INSERT OR UPDATE ON spouse_of
    FOR EACH ROW
    EXECUTE FUNCTION check_spouse_gender_different();



-- 3. Добавим индекс для spouse_of по person2_id
CREATE INDEX idx_spouse_of_person2 ON spouse_of(person2_id);

-- 4. Удобное симметричное представление супругов (spouse)
CREATE VIEW v_spouse_symmetric AS
    SELECT person1_id AS person_id, person2_id AS spouse_id FROM spouse_of
    UNION ALL
    SELECT person2_id AS person_id, person1_id AS spouse_id FROM spouse_of;

-- 5. Такое же представление для братьев-сестёр (sibling)
CREATE VIEW v_sibling_symmetric AS
    SELECT person1_id AS person_id, person2_id AS sibling_id FROM sibling_of
    UNION ALL
    SELECT person2_id AS person_id, person1_id AS sibling_id FROM sibling_of;

-- 6. Примеры запросов через представления 4-5:
-- SELECT spouse_id FROM v_spouse_symmetric WHERE person_id = 'P404303';
-- SELECT sibling_id FROM v_sibling_symmetric WHERE person_id = 'P403397';
