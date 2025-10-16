-- Pergunta 1: Qual a média de valor total (total_amount) recebido em um mês
-- considerando todos os yellow táxis da frota?
-- A consulta agrupa por ano e mês para fornecer a média mensal.

SELECT
     pickup_year
    ,pickup_month
    ,ROUND(AVG(total_amount), 2) AS average_total_amount
FROM taxi_silver
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month;

-- Pergunta 2: Qual a média de passageiros (passenger_count) por cada hora do dia
-- que pegaram táxi no mês de maio considerando todos os táxis da frota?

SELECT
     hour(pickup_datetime) AS pickup_hour
    ,ROUND(AVG(passenger_count), 2) AS average_passenger_count
FROM taxi_silver
WHERE pickup_month = 5 AND pickup_year = 2023
GROUP BY pickup_hour
ORDER BY pickup_hour;