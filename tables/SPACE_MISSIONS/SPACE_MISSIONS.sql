CREATE TABLE PUBLISH_D.SPACE_MISSIONS (
    mission_date DATE,
    mission_id VARCHAR(20),
    destination VARCHAR(50),
    status VARCHAR(50),
    crew_size INT,
    duration_days INT,
    success_rate DECIMAL(5,2),
    security_code VARCHAR(20)
);
