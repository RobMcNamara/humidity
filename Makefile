homecontrolmake: main.c
	gcc -o humidity main.c raspberry-pi-bme280/bme280.c -std=c99 -lrdkafka -lwiringPi -lm