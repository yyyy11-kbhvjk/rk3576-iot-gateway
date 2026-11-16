#include "MqttClient.h"

#include <chrono>
#include <iostream>
#include <thread>
#include <utility>

using namespace std::chrono_literals;

MqttClient::MqttClient(
    std::string client_id,
    std::string host,
    int port,
    std::string topic
)
    : client_id_(std::move(client_id)),
      host_(std::move(host)),
      port_(port),
      topic_(std::move(topic))
{
}

MqttClient::~MqttClient()
{
    stop();
}

void MqttClient::scheduleReconnect()
{
    reconnect_pending_ = true;
    next_retry_ =
        std::chrono::steady_clock::now() + 2s;
}

bool MqttClient::start(MessageHandler handler)
{
    if (library_initialized_) {
        return false;
    }

    handler_ = std::move(handler);

    if (!handler_) {
        std::cerr << "未设置消息处理函数\n";
        return false;
    }

    int rc = mosquitto_lib_init();

    if (rc != MOSQ_ERR_SUCCESS) {
        std::cerr << "Mosquitto初始化失败\n";
        return false;
    }

    library_initialized_ = true;
    callback_error_ = false;
    reconnect_pending_ = false;

    mosq_ = mosquitto_new(
        client_id_.c_str(), true, this
    );

    if (!mosq_) {
        stop();
        return false;
    }

    mosquitto_connect_callback_set(
        mosq_, onConnect
    );

    mosquitto_message_callback_set(
        mosq_, onMessage
    );

    rc = mosquitto_connect(
        mosq_, host_.c_str(), port_, 60
    );

    if (rc != MOSQ_ERR_SUCCESS) {
        std::cerr
            << "初始连接失败，进入重连模式："
            << mosquitto_strerror(rc) << '\n';

        scheduleReconnect();
    } else {
        std::cout << "MQTT客户端启动成功\n";
    }

    // Broker暂时不可用也不退出
    return true;
}

void MqttClient::onConnect(
    mosquitto* mosq,
    void* userdata,
    int rc
)
{
    auto* self =
        static_cast<MqttClient*>(userdata);

    if (!self) return;

    if (rc != 0) {
        std::cerr << "MQTT握手失败："
                  << rc << '\n';
        self->callback_error_ = true;
        return;
    }

    self->reconnect_pending_ = false;

    std::cout << "MQTT连接成功！\n";

    // 每次重连成功后重新订阅
    int result = mosquitto_subscribe(
        mosq,
        nullptr,
        self->topic_.c_str(),
        0
    );

    if (result == MOSQ_ERR_SUCCESS) {
        std::cout
            << "MQTT订阅请求已发送："
            << self->topic_ << '\n';
    } else {
        std::cerr << "订阅失败："
                  << mosquitto_strerror(result)
                  << '\n';
        self->callback_error_ = true;
    }
}

void MqttClient::onMessage(
    mosquitto*,
    void* userdata,
    const mosquitto_message* msg
)
{
    auto* self =
        static_cast<MqttClient*>(userdata);

    if (!self || !msg ||
        !msg->payload || msg->payloadlen <= 0) {
        return;
    }

    std::string payload(
        static_cast<const char*>(msg->payload),
        static_cast<size_t>(msg->payloadlen)
    );

    try {
        self->handler_(payload);
    } catch (const std::exception& e) {
        std::cerr
            << "消息处理异常："
            << e.what() << '\n';
        self->callback_error_ = true;
    }
}

bool MqttClient::poll()
{
    if (!mosq_ || callback_error_) {
        return false;
    }

    if (reconnect_pending_) {
        auto now =
            std::chrono::steady_clock::now();

        if (now < next_retry_) {
            std::this_thread::sleep_for(200ms);
            return true;
        }

        std::cout << "尝试重新连接MQTT...\n";

        int rc = mosquitto_reconnect(mosq_);

        if (rc != MOSQ_ERR_SUCCESS) {
            std::cerr
                << "重连失败："
                << mosquitto_strerror(rc)
                << '\n';

            scheduleReconnect();
            return true;
        }

        reconnect_pending_ = false;

        std::cout
            << "TCP重连成功，等待MQTT握手\n";
    }

    int rc = mosquitto_loop(
        mosq_, 500, 1
    );

    if (callback_error_) {
        return false;
    }

    if (rc == MOSQ_ERR_SUCCESS) {
        return true;
    }

    // 可恢复的网络异常
    if (rc == MOSQ_ERR_CONN_LOST ||
        rc == MOSQ_ERR_NO_CONN ||
        rc == MOSQ_ERR_ERRNO) {

        std::cerr
            << "MQTT连接中断："
            << mosquitto_strerror(rc)
            << '\n';

        scheduleReconnect();
        return true;
    }

    // 其他异常不盲目重试
    std::cerr << "MQTT不可恢复错误："
              << mosquitto_strerror(rc)
              << '\n';
    return false;
}

void MqttClient::stop()
{
    if (mosq_) {
        mosquitto_disconnect(mosq_);
        mosquitto_destroy(mosq_);
        mosq_ = nullptr;
    }

    if (library_initialized_) {
        mosquitto_lib_cleanup();
        library_initialized_ = false;
    }

    reconnect_pending_ = false;
}
