echo "Starting TZUpdater"
java -jar /var/www/html/WeatherData4.0/lib/tzupdater-2.1.2/tzupdater.jar -l http://www.iana.org/time-zones/repository/tzdata-latest.tar.gz
echo "TZUpdater finished"