select * 
from coviddeathsdata
order by date;



-- Convert data
SET SQL_SAFE_UPDATES = 0;

UPDATE coviddeathsdata 
SET date = STR_TO_DATE(
    -- Add leading zeros if missing
    CONCAT(
        LPAD(SUBSTRING_INDEX(date, '/', 1), 2, '0'), '/',
        LPAD(SUBSTRING_INDEX(SUBSTRING_INDEX(date, '/', 2), '/', -1), 2, '0'), '/',
        SUBSTRING_INDEX(date, '/', -1)
    ),
    '%m/%d/%Y'
);

SET SQL_SAFE_UPDATES = 1;

UPDATE coviddeathsdata
SET date = STR_TO_DATE(date, '%d/%m/%Y')
WHERE date LIKE '%/%/%';

select location,date,total_cases,total_deaths,population
from coviddeathsdata
where continent !=''
order by 1,2;

-- likehood of dying if you got covid in your country
select location,date,total_cases,total_deaths,(total_deaths/total_cases)*100 as death_rate 
from coviddeathsdata
where continent !='' and location like'%state%'
order by 1,2;

-- total cases vs population
select location,date,total_cases,total_deaths,(total_cases/population)*100 as percenatgeofpopulationinfected 
from coviddeathsdata
where continent !='' and location like'%state%'
order by 1,2;

-- highest infection rate to population
select location,population,max(total_cases) as highestinfectioncount,max((total_cases/population)*100) as highestinfected
from coviddeathsdata
-- where location like'%state%'
where continent !=''
group by location,population
order by highestinfected desc;

-- highest death rate to population
select location,max(cast(total_deaths as signed)) as total_death_count
from coviddeathsdata
-- where location like'%state%'
where continent !=''
group by location
order by total_death_count desc;


-- highest death rate to population by contient
select continent,max(cast(total_deaths as signed)) as total_death_count
from coviddeathsdata
-- where location like'%state%'
where continent !=''
group by continent
order by total_death_count desc;

-- contient highest death rate to population
select continent,max(cast(total_deaths as signed)) as total_death_count
from coviddeathsdata
-- where location like'%state%'
where continent !=''
group by continent
order by total_death_count desc;

-- contient highest infection rate to population
select continent,location,population,max(total_cases) as highestinfectioncount,max((total_cases/population)*100) as highestinfectedrate
from coviddeathsdata
-- where location like'%state%'
where continent !=''
group by continent,population,location
order by highestinfectedrate desc;

-- global numbers 
-- likehood of dying if you got covid in your country
select date,sum(new_cases) as total_cases,sum(new_deaths) as total_death,sum(new_deaths)/sum(new_cases)*100 as death_rate 
from coviddeathsdata
where continent !='' or continent is not null
-- location like'%state%'
group by date
order by 1,2;

select *
from covidvaccines
order by date;

UPDATE covidvaccines 
SET date = STR_TO_DATE(
    -- Add leading zeros if missing
    CONCAT(
        LPAD(SUBSTRING_INDEX(date, '/', 1), 2, '0'), '/',
        LPAD(SUBSTRING_INDEX(SUBSTRING_INDEX(date, '/', 2), '/', -1), 2, '0'), '/',
        SUBSTRING_INDEX(date, '/', -1)
    ),
    '%d/%m/%Y'
);
-- total population vs vacciation 
select dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations,
sum(vac.new_vaccinations) over (partition by dea.location order by dea.location,dea.date) as rolling_vaccinated
from coviddeathsdata dea
join covidvaccines vac
	on dea.location=vac.location
	and dea.date=vac.date
    where dea.continent!='' 
    order by 2,3;
    
    -- use cte
    with PopvsVac(continent,location,date,population,new_vaccinations,rolling_vaccinated)
    as
    (
	select dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations,
	sum(vac.new_vaccinations) over (partition by dea.location order by dea.location,dea.date) as rolling_vaccinated
	from coviddeathsdata dea
	join covidvaccines vac
		on dea.location=vac.location
		and dea.date=vac.date
    where dea.continent!='' 
    )
    SELECT * 
FROM PopvsVac
ORDER BY location, date;

CREATE TABLE percentpopulationvaccinated (
    continent VARCHAR(255),
    location VARCHAR(255),
    date DATE,
    population BIGINT,
    new_vaccinatipercentpopulationvaccinatedons DECIMAL(20,2),
    rolling_vaccinated DECIMAL(20,2)
);

INSERT INTO percentpopulationvaccinated 
(continent, location, date, population, new_vaccinations, rolling_vaccinated)
SELECT 
    dea.continent,
    dea.location,
    dea.date,
    dea.population,
    NULLIF(vac.new_vaccinations, '') as new_vaccinations,
    SUM(NULLIF(vac.new_vaccinations, '')) OVER (
        PARTITION BY dea.location 
        ORDER BY dea.location, dea.date
    ) as rolling_vaccinated
FROM coviddeathsdata dea
JOIN covidvaccines vac
    ON dea.location = vac.location
    AND dea.date = vac.date;
-- WHERE dea.continent != ''

DROP TABLE percentpopulationvaccinated;

CREATE OR REPLACE VIEW percentpopulationvaccinated AS
select dea.continent,dea.location,dea.date,dea.population,vac.new_vaccinations,
	sum(vac.new_vaccinations) over (partition by dea.location order by dea.location,dea.date) as rolling_vaccinated
	from coviddeathsdata dea
	join covidvaccines vac
		on dea.location=vac.location
		and dea.date=vac.date
    where dea.continent!=''
ORDER BY location, date;

SELECT * FROM portfolio.percentpopulationvaccinated;