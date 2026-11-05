// Прототип: реплика состояния соседа и применение её при переходе standby -> active.
// Одинаков на обоих узлах. Сосед через mosquitto bridge складывает свои значения
// в /replica/remote/..., здесь они только запоминаются. Свое дерево узел
// обновляет из реплики ЕДИНСТВЕННЫЙ раз - в момент перехода в роль active.
// Что реплицируется, определяют строки topic в wb-ha-bridge.conf соседа: сюда
// приходит только то, что пропустил его мост, отдельного списка здесь нет.
// Годность реплики (канал от соседа жив) проверяет keepalived скриптом
// wb-ha-replica-gate.sh, здесь это не отслеживается.

var replica = {};         // "<device>/<control>" -> строка-значение из реплики
var prevRole = "unknown";

// /replica/remote/devices/<device>/controls/<control>
trackMqtt("/replica/+/devices/+/controls/+", function (message) {
    var parts = message.topic.split("/");
    replica[parts[4] + "/" + parts[6]] = message.value;
});

// Приводит строку из реплики к типу, который у control'а сейчас в дереве:
// switch -> boolean, value/range -> number, text -> строка.
function convertLike(current, raw) {
    if (typeof current === "boolean") {
        return raw === "1" || raw === "true";
    }
    if (typeof current === "number") {
        return Number(raw);
    }
    return raw;
}

function applyReplica() {
    var applied = 0;
    var skipped = 0;
    for (var key in replica) {
        var current = dev[key];
        // Нет такого control'а, кнопка (pushbutton) или только чтение
        // (датчики, входы, счётчики): писать нечего.
        if (current === undefined || current === null || typeof current === "object" ||
            dev[key + "#readonly"] === true) {
            skipped++;
            continue;
        }
        var raw = replica[key];
        try {
            dev[key] = convertLike(current, raw);
            log("wbreplica: применили из реплики {} = {}", key, raw);
            applied++;
        } catch (e) {
            log("wbreplica: не удалось применить {}: {}", key, e);
        }
    }
    log("wbreplica: применено {}, пропущено (только чтение, кнопки) {}", applied, skipped);
    if (applied === 0 && skipped === 0) {
        log("wbreplica: реплика пуста, применять нечего");
    }
}

function onRole(role) {
    if (role === prevRole) {
        return;
    }
    if (role === "active" && (prevRole === "standby" || prevRole === "fault")) {
        log("wbreplica: переход {} -> active, применяем реплику", prevRole);
        applyReplica();
    }
    prevRole = role;
}

trackMqtt("/devices/system/controls/role", function (message) {
    onRole(message.value);
});

// После рестарта wb-rules подписка не получает retained-значение роли (и cron
// перепубликует его только раз в минуту), поэтому начальную роль читаем один
// раз отдельной командой - иначе переход сразу после рестарта терялся бы.
runShellCommand("mosquitto_sub -t '/devices/system/controls/role' -C 1 -W 2", {
    captureOutput: true,
    exitCallback: function (exitCode, capturedOutput) {
        var initial = String(capturedOutput || "").replace(/\s+/g, "");
        if (prevRole === "unknown" && initial) {
            prevRole = initial;
            log("wbreplica: начальная роль = {}", initial);
        }
    }
});
