PRAGMA foreign_keys = ON;

CREATE TABLE departments (
    department_id INTEGER PRIMARY KEY,
    department_name TEXT NOT NULL UNIQUE,
    cost_center TEXT NOT NULL UNIQUE
);

CREATE TABLE employees (
    employee_id INTEGER PRIMARY KEY,
    department_id INTEGER NOT NULL,
    manager_id INTEGER,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    title TEXT NOT NULL,
    hire_date TEXT NOT NULL,
    country TEXT NOT NULL,
    employment_status TEXT NOT NULL,
    FOREIGN KEY (department_id) REFERENCES departments(department_id),
    FOREIGN KEY (manager_id) REFERENCES employees(employee_id)
);

CREATE TABLE salaries (
    salary_id INTEGER PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    effective_date TEXT NOT NULL,
    annual_salary_cents INTEGER NOT NULL,
    bonus_target REAL NOT NULL,
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

CREATE TABLE performance_reviews (
    review_id INTEGER PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    review_period TEXT NOT NULL,
    reviewer_id INTEGER NOT NULL,
    rating INTEGER NOT NULL,
    review_summary TEXT NOT NULL,
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
    FOREIGN KEY (reviewer_id) REFERENCES employees(employee_id)
);

CREATE INDEX idx_employees_department_id ON employees(department_id);
CREATE INDEX idx_employees_manager_id ON employees(manager_id);
CREATE INDEX idx_salaries_employee_id ON salaries(employee_id);
CREATE INDEX idx_reviews_employee_id ON performance_reviews(employee_id);

