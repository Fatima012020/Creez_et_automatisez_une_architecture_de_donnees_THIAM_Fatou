-- ============================================================
-- PROJET : Sport Data Solution
-- FICHIER : 01_create_tables.sql
-- OBJECTIF :
--     Créer les quatre tables nécessaires au POC.
--
-- TABLES :
--     1. employees
--     2. sport_profile
--     3. activities
--     4. commute_validation
--
-- À cette étape, aucune donnée n'est insérée.
-- ============================================================


-- ============================================================
-- 1. TABLE EMPLOYEES
-- ============================================================
-- Table centrale du projet.
-- Elle contiendra les informations provenant du fichier
-- Donnees_RH.xlsx.
-- ============================================================

CREATE TABLE employees (

    -- Identifiant unique du salarié.
    employee_id INTEGER PRIMARY KEY,

    -- Informations personnelles.
    last_name VARCHAR(100) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    birth_date DATE NOT NULL,

    -- Business Unit du salarié.
    business_unit VARCHAR(100) NOT NULL,

    -- Informations liées à l'emploi.
    hire_date DATE NOT NULL,

    -- Salaire annuel brut.
    -- NUMERIC est utilisé pour conserver précisément
    -- les valeurs monétaires.
    gross_salary NUMERIC(10, 2) NOT NULL
        CHECK (gross_salary > 0),

    contract_type VARCHAR(20) NOT NULL,

    -- Le nombre de jours de congés ne peut pas être négatif.
    paid_leave_days INTEGER NOT NULL
        CHECK (paid_leave_days >= 0),

    -- Informations nécessaires au contrôle du trajet.
    home_address TEXT NOT NULL,
    commute_mode VARCHAR(100) NOT NULL
);


-- ============================================================
-- 2. TABLE SPORT_PROFILE
-- ============================================================
-- Contient la pratique sportive déclarée dans
-- Donnees_Sportive.xlsx.
--
-- employee_id est à la fois :
--     - clé primaire : un seul profil par salarié ;
--     - clé étrangère : le salarié doit exister dans employees.
--
-- sport_type peut être NULL car certains salariés n'ont
-- aucune pratique sportive renseignée.
-- ============================================================

CREATE TABLE sport_profile (

    employee_id INTEGER PRIMARY KEY,

    sport_type VARCHAR(100),

    CONSTRAINT fk_sport_profile_employee
        FOREIGN KEY (employee_id)
        REFERENCES employees(employee_id)
);


-- ============================================================
-- 3. TABLE ACTIVITIES
-- ============================================================
-- Contient l'historique des activités physiques.
--
-- Ces données seront simulées sur les 12 derniers mois
-- afin de représenter les futures données d'une application
-- sportive telle que Strava.
-- ============================================================

CREATE TABLE activities (

    -- Identifiant unique de l'activité.
    activity_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Salarié ayant réalisé l'activité.
    employee_id INTEGER NOT NULL,

    -- Date et heure de début.
    start_datetime TIMESTAMP NOT NULL,

    -- Type d'activité : Running, Tennis, Natation, etc.
    sport_type VARCHAR(100) NOT NULL,

    -- Distance exprimée en mètres.
    -- NULL est autorisé lorsque la distance n'est pas
    -- pertinente pour l'activité.
    distance_meters NUMERIC(10, 2)
        CHECK (distance_meters >= 0),

    -- Date et heure de fin.
    end_datetime TIMESTAMP NOT NULL,

    -- Commentaire facultatif associé à l'activité.
    comment TEXT,

    -- Vérifie qu'une activité ne se termine pas
    -- avant son heure de début.
    CONSTRAINT chk_activity_dates
        CHECK (end_datetime > start_datetime),

    -- Chaque activité doit appartenir à un salarié existant.
    CONSTRAINT fk_activity_employee
        FOREIGN KEY (employee_id)
        REFERENCES employees(employee_id)
);


-- ============================================================
-- 4. TABLE COMMUTE_VALIDATION
-- ============================================================
-- Contient le résultat du contrôle du trajet entre
-- le domicile du salarié et l'entreprise.
--
-- Cette table sera alimentée plus tard après calcul
-- de la distance domicile -> bureau.
-- ============================================================

CREATE TABLE commute_validation (

    -- Un seul résultat de validation courant par salarié.
    employee_id INTEGER PRIMARY KEY,

    -- Distance domicile -> entreprise en kilomètres.
    distance_km NUMERIC(10, 2) NOT NULL
        CHECK (distance_km >= 0),

    -- Seuil applicable au trajet sportif.
    -- NULL est autorisé lorsqu'aucun seuil sportif
    -- n'est applicable au moyen de déplacement.
    threshold_km NUMERIC(10, 2),

    -- Indique si le moyen de déplacement déclaré
    -- correspond à un déplacement sportif.
    is_sport_commute BOOLEAN NOT NULL,

    -- Indique si la déclaration est cohérente avec
    -- les règles de distance du POC.
    is_valid BOOLEAN NOT NULL,

    -- Explication facultative du résultat.
    validation_reason TEXT,

    -- Le salarié doit exister dans employees.
    CONSTRAINT fk_commute_employee
        FOREIGN KEY (employee_id)
        REFERENCES employees(employee_id),

    -- Si un seuil existe, il ne peut pas être négatif.
    CONSTRAINT chk_commute_threshold
        CHECK (threshold_km IS NULL OR threshold_km >= 0)
);