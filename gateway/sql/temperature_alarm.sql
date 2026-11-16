-- 告警事件表
CREATE TABLE IF NOT EXISTS alarm_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telemetry_id INTEGER NOT NULL,
    device TEXT NOT NULL,
    event_type TEXT NOT NULL,
    temp_c REAL NOT NULL,
    created_at TEXT NOT NULL DEFAULT
        (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- 保存每台设备当前的告警状态
CREATE TABLE IF NOT EXISTS alarm_state (
    device TEXT PRIMARY KEY,
    active INTEGER NOT NULL DEFAULT 0
);

-- 每当采集数据写入数据库时自动判断
CREATE TRIGGER IF NOT EXISTS trg_temperature_alarm
AFTER INSERT ON telemetry
BEGIN
    INSERT OR IGNORE INTO alarm_state(device, active)
    VALUES (NEW.device, 0);

    -- 首次进入高温状态时产生告警
    INSERT INTO alarm_events(
        telemetry_id, device, event_type, temp_c
    )
    SELECT NEW.id, NEW.device, 'HIGH_TEMP', NEW.temp_c
    WHERE NEW.temp_c >= 26.0
      AND (
          SELECT active FROM alarm_state
          WHERE device = NEW.device
      ) = 0;

    -- 温度恢复到25℃及以下时产生恢复事件
    INSERT INTO alarm_events(
        telemetry_id, device, event_type, temp_c
    )
    SELECT NEW.id, NEW.device, 'RECOVERED', NEW.temp_c
    WHERE NEW.temp_c <= 25.0
      AND (
          SELECT active FROM alarm_state
          WHERE device = NEW.device
      ) = 1;

    -- 更新状态
    UPDATE alarm_state
    SET active = CASE
        WHEN NEW.temp_c >= 26.0 THEN 1
        WHEN NEW.temp_c <= 25.0 THEN 0
        ELSE active
    END
    WHERE device = NEW.device;
END;
