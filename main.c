#include <stdio.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>
#include <stdbool.h>
#include <string.h>

#include <librdkafka/rdkafka.h>
#include "raspberry-pi-bme280/bme280.h"

#define COUNTER 10;
#define SLEEP_PERIOD 5

static volatile sig_atomic_t run = 1;

void print_date_time()
{
        time_t tvar;
        /* Returns the current time of the system as a time_t object
     * which is usually time since an epoch, typically the Unix epoch
     */
        time(&tvar);
        /* Print the date and time after converting a time_t object
     * to a textual representation using ctime()
     */
        printf("Today's date and time : %s", ctime(&tvar));
}

struct read_value
{
        int humidity;
        int temp;
};

struct read_value get_humidity_and_temp()
{
        struct read_value reads;
        reads.humidity = 1;
        reads.temp = 1;
        return reads;
}

/**
 * @brief Message delivery report callback.
 *
 * This callback is called exactly once per message, indicating if
 * the message was succesfully delivered
 * (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) or permanently
 * failed delivery (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR).
 *
 * The callback is triggered from rd_kafka_poll() and executes on
 * the application's thread.
 */
static void dr_msg_cb(rd_kafka_t *rk,
                      const rd_kafka_message_t *rkmessage, void *opaque)
{
        if (rkmessage->err)
                fprintf(stderr, "%% Message delivery failed: %s\n",
                        rd_kafka_err2str(rkmessage->err));
        else
                fprintf(stderr,
                        "%% Message delivered (%zd bytes, "
                        "partition %" PRId32 ")\n",
                        rkmessage->len, rkmessage->partition);

        /* The rkmessage is destroyed automatically by librdkafka */
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
        struct read_value s;

        rd_kafka_t *producer;
        rd_kafka_conf_t *configuration;

        char errstring[512];
        char buf[512];

        const char *brokers; /* Argument: broker list */
        const char *topic;

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

        if (argc != 3)
        {
                fprintf(stderr, "%% Usage: %s <broker> <topic>\n", argv[0]);
                return 1;
        }

        brokers = argv[1];
        topic = argv[2];

        configuration = rd_kafka_conf_new();

        if (rd_kafka_conf_set(configuration, "bootstrap.servers", brokers, errstring, sizeof(errstring)) != RD_KAFKA_CONF_OK)
        {
                fprintf(stderr, "Problem setting config: %s\n", errstring);
                return 1;
        }

        rd_kafka_conf_set_dr_msg_cb(configuration, dr_msg_cb);

        producer = rd_kafka_new(RD_KAFKA_PRODUCER, configuration, errstring, sizeof(errstring));

        if (!producer)
        {
                fprintf(stderr,
                        "%% Failed to create new producer: %s\n", errstring);
                return 1;
        }

        signal(SIGINT, stop);

        while (run && true)
        {
                print_date_time();
                //     s = get_humidity_and_temp();
                //     printf("Temp: %d\nHumidity: %d\n", s.temp, s.humidity);
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

                size_t len = strlen(buf);
                rd_kafka_resp_err_t err;

                if (buf[len - 1] == '\n') /* Remove newline */
                        buf[--len] = '\0';

                if (len == 0)
                {
                        /* Empty line: only serve delivery reports */
                        rd_kafka_poll(producer, 0 /*non-blocking */);
                        continue;
                }

                /*
                 * Send/Produce message.
                 * This is an asynchronous call, on success it will only
                 * enqueue the message on the internal producer queue.
                 * The actual delivery attempts to the broker are handled
                 * by background threads.
                 * The previously registered delivery report callback
                 * (dr_msg_cb) is used to signal back to the application
                 * when the message has been delivered (or failed).
                 */
        retry:
                err = rd_kafka_producev(
                    /* Producer handle */
                    producer,
                    /* Topic name */
                    RD_KAFKA_V_TOPIC(topic),
                    /* Make a copy of the payload. */
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                    /* Message value and length */
                    RD_KAFKA_V_VALUE(buf, len),
                    /* Per-Message opaque, provided in
                         * delivery report callback as
                         * msg_opaque. */
                    RD_KAFKA_V_OPAQUE(NULL),
                    /* End sentinel */
                    RD_KAFKA_V_END);

                if (err)
                {
                        /*
                         * Failed to *enqueue* message for producing.
                         */
                        fprintf(stderr,
                                "%% Failed to produce to topic %s: %s\n",
                                topic, rd_kafka_err2str(err));

                        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL)
                        {
                                /* If the internal queue is full, wait for
                                 * messages to be delivered and then retry.
                                 * The internal queue represents both
                                 * messages to be sent and messages that have
                                 * been sent or failed, awaiting their
                                 * delivery report callback to be called.
                                 *
                                 * The internal queue is limited by the
                                 * configuration property
                                 * queue.buffering.max.messages */
                                rd_kafka_poll(producer, 1000 /*block for max 1000ms*/);
                                goto retry;
                        }
                }
                else
                {
                        fprintf(stderr, "%% Enqueued message (%zd bytes) "
                                        "for topic %s\n",
                                len, topic);
                }

                /* A producer application should continually serve
                 * the delivery report queue by calling rd_kafka_poll()
                 * at frequent intervals.
                 * Either put the poll call in your main loop, or in a
                 * dedicated thread, or call it after every
                 * rd_kafka_produce() call.
                 * Just make sure that rd_kafka_poll() is still called
                 * during periods where you are not producing any messages
                 * to make sure previously produced messages have their
                 * delivery report callback served (and any other callbacks
                 * you register). */
                rd_kafka_poll(producer, 0 /*non-blocking*/);
        }

        /* Wait for final messages to be delivered or fail.
         * rd_kafka_flush() is an abstraction over rd_kafka_poll() which
         * waits for all messages to be delivered. */
        fprintf(stderr, "%% Flushing final messages..\n");
        rd_kafka_flush(producer, 10 * 1000 /* wait for max 10 seconds */);

        /* If the output queue is still not empty there is an issue
         * with producing messages to the clusters. */
        if (rd_kafka_outq_len(producer) > 0)
                fprintf(stderr, "%% %d message(s) were not delivered\n",
                        rd_kafka_outq_len(producer));

        /* Destroy the producer instance */
        rd_kafka_destroy(producer);

        return 0;
}