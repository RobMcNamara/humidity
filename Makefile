homecontrolmake: main.c
	gcc -o humidity main_no_kafka.c raspberry-pi-bme280/bme280.c -std=c99 -lrdkafka -lwiringPi -lm

