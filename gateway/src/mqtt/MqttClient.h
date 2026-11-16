#pragma once

#include <chrono>
#include <functional>
#include <string>
#include <mosquitto.h>

class MqttClient {
public:
    using MessageHandler =
        std::function<void(const std::string&)>;

    MqttClient(
        std::string client_id,
        std::string host,
        int port,
        std::string topic
    );

    ~MqttClient();

    MqttClient(const MqttClient&) = delete;
    MqttClient& operator=(const MqttClient&) = delete;

    bool start(MessageHandler handler);
    bool poll();
    void stop();

private:
    static void onConnect(
        mosquitto* mosq,
        void* userdata,
        int rc
    );

    static void onMessage(
        mosquitto* mosq,
        void* userdata,
        const mosquitto_message* msg
    );

    void scheduleReconnect();

    std::string client_id_;
    std::string host_;
    int port_;
    std::string topic_;

    MessageHandler handler_;

    mosquitto* mosq_ = nullptr;

    bool library_initialized_ = false;
    bool callback_error_ = false;
    bool reconnect_pending_ = false;

    std::chrono::steady_clock::time_point
        next_retry_{};
};
