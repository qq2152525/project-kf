mkdir -p ../log
kill `cat ../log/mongp.pid`
./import-csv-to-mongo.coffee ~/2000W > ../log/mongp.log 2>&1 &
echo $!>../log/mongp.pid
tail -f ../log/mongp.log

