#include <stdio.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>
#include <stdbool.h>
#include <string.h>

#include "raspberry-pi-bme280/bme280.h"

#define COUNTER 10;
#define SLEEP_PERIOD 1800

static volatile sig_atomic_t run = 1;

void print_date_time()
{
        time_t tvar;

        time(&tvar);

        printf("Today's date and time : %s", ctime(&tvar));
}

//nice pattern, keep this.
static void stop(int sig)
{
        run = 0;
        fclose(stdin); /* abort fgets() */
}

int main(int argc, char *argv[])
{
        printf("Starting up node: %s\n", argv[1]);

        char errstring[512];
        char buf[512];

        int fd = wiringPiI2CSetup(BME280_ADDRESS);
        if (fd < 0)
        {
                printf("Device not found");
                return -1;
        }

        bme280_calib_data cal;
        readCalibrationData(fd, &cal);

        wiringPiI2CWriteReg8(fd, 0xf2, 0x01); // humidity oversampling x 1
        wiringPiI2CWriteReg8(fd, 0xf4, 0x25); // pressure and temperature oversampling x 1, mode normal

        bme280_raw_data raw;

        const char *brokers; /* Argument: broker list */
        const char *topic;

        if (argc != 3)
        {
                fprintf(stderr, "%% Usage: %s <broker:port> <topic>\n", argv[0]);
                return 1;
        }

        signal(SIGINT, stop);

        while (run && true)
        {

                sleep(SLEEP_PERIOD);

                getRawData(fd, &raw);

                int32_t t_fine = getTemperatureCalibration(&cal, raw.temperature);
                float t = compensateTemperature(t_fine);                        // C
                float p = compensatePressure(raw.pressure, &cal, t_fine) / 100; // hPa
                float h = compensateHumidity(raw.humidity, &cal, t_fine);       // %
                float a = getAltitude(p);                                       // meters

                snprintf(buf, sizeof(buf), "{\"sensor\":\"bme280\", \"humidity\":%.2f, \"pressure\":%.2f,"
                                           " \"temperature\":%.2f, \"altitude\":%.2f, \"timestamp\":%d}\n",
                         h, p, t, a, (int)time(NULL));

                printf("%s\n", buf);

                size_t len = strlen(buf);

                if (buf[len - 1] == '\n') /* Remove newline */
                        buf[--len] = '\0';
        }
        return 0;
}
