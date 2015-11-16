REGISTER file:/usr/lib/pig/piggybank.jar;

DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

SET default_parallel 12;

-- load dataset first time
Data1 = LOAD 's3://cs6240northvirginia/input/data.csv' using CSVLoader();
-- load dataset first time
Data2 = LOAD 's3://cs6240northvirginia/input/data.csv' using CSVLoader();


-- parsing DataSet and generating the data for flights departure from ORD, not cancelled, not diverted
FirstFlight = FILTER Data1 BY ($11 == 'ORD' AND $17 != 'JFK') AND ($41 == '0.00' AND $43 == '0.00')
					AND (ToDate($5, 'yyyy-MM-dd')>ToDate('2007-05-31','yyyy-MM-dd'))
					AND (ToDate($5, 'yyyy-MM-dd')<ToDate('2008-06-01','yyyy-MM-dd'));

-- parsing DataSet and generating the data for flights fly to JFK, not cancelled, not diverted
SecondFlight = FILTER Data2 BY ($11 != 'ORD' AND $17 == 'JFK') AND ($41 == '0.00' AND $43 == '0.00')
					AND (ToDate($5, 'yyyy-MM-dd')>ToDate('2007-05-31','yyyy-MM-dd'))
					AND (ToDate($5, 'yyyy-MM-dd')<ToDate('2008-06-01','yyyy-MM-dd'));


-- generating the data for Flights1 with useful field
F1 = FOREACH FirstFlight GENERATE $5 as FirstDate, $17 as Dest, (int)$35 as arrTime, $37 as first_delay;

-- generating the data for Filghts2 with useful field
F2 = FOREACH SecondFlight GENERATE $5 as SecondDate, $11 as Origin, (int)$24 as DepTime, $37 as second_delay;


-- join FirstFlight and SecondFlight based on FlightDate and Destination
JoinFlights = JOIN F1 BY (FirstDate, Dest), F2 BY (SecondDate, Origin);


-- filter out all qualifying flighs pairs fall into required period.
FilterJointFlights = FILTER JoinFlights BY arrTime < DepTime;

-- calculate the total delay for each pair
DelayDataSet = FOREACH FilterJointFlights GENERATE (float)(first_delay + second_delay) as delayTime;

-- group all DelayDataSet
GroupDelay = GROUP DelayDataSet ALL;
-- generate the average delay time
AvgDelay = FOREACH GroupDelay GENERATE AVG(DelayDataSet.delayTime);

-- output average delay time to S3 directory as following
STORE AvgDelay INTO 's3://cs6240northvirginia/output/FilterFirst_Output';


