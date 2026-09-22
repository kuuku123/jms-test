/**
 * JEUS 8.5 JMS Test Studio - Client Application Logic
 * Adheres to strict DOM security rules: zero innerHTML usage, textContent only.
 */

(function () {
    "use strict";

    // Application state
    const state = {
        cfJndi: "ConnectionFactory",
        destJndi: "ExamplesQueue",
        listenerRunning: false,
        listeningDest: "",
        autoRefreshInterval: null,
        lastBrowsedMessages: []
    };

    // DOM Elements
    const elements = {
        inputCfJndi: document.getElementById("input-cf-jndi"),
        inputDestJndi: document.getElementById("input-dest-jndi"),
        presetCf: document.getElementById("preset-cf"),
        presetDest: document.getElementById("preset-dest"),
        btnTestJndi: document.getElementById("btn-test-jndi"),
        btnSaveJndi: document.getElementById("btn-save-jndi"),
        jndiResultBox: document.getElementById("jndi-result-box"),
        jndiResultText: document.getElementById("jndi-result-text"),
        listenerBadge: document.getElementById("listener-status-badge"),

        // Tabs
        tabButtons: document.querySelectorAll(".tab-btn"),
        tabPanes: document.querySelectorAll(".tab-pane"),

        // Queue Studio
        formSendQueue: document.getElementById("form-send-queue"),
        queuePayload: document.getElementById("queue-payload"),
        queuePriority: document.getElementById("queue-priority"),
        queueTtl: document.getElementById("queue-ttl"),
        queueCorrelation: document.getElementById("queue-correlation"),
        propertiesContainer: document.getElementById("properties-container"),
        btnAddProperty: document.getElementById("btn-add-property"),
        btnSendBatch: document.getElementById("btn-send-batch"),
        browserSelector: document.getElementById("browser-selector"),
        btnBrowseQueue: document.getElementById("btn-browse-queue"),
        btnReceiveQueue: document.getElementById("btn-receive-queue"),
        btnToggleListener: document.getElementById("btn-toggle-listener"),
        browseCountLabel: document.getElementById("browse-count-label"),
        browseTableBody: document.getElementById("browse-table-body"),
        btnClearBrowseView: document.getElementById("btn-clear-browse-view"),

        // Topic Studio
        formPublishTopic: document.getElementById("form-publish-topic"),
        topicPayload: document.getElementById("topic-payload"),
        topicPriority: document.getElementById("topic-priority"),
        topicTtl: document.getElementById("topic-ttl"),
        topicCorrelation: document.getElementById("topic-correlation"),

        // Activities
        activityTableBody: document.getElementById("activity-table-body"),
        checkAutoRefresh: document.getElementById("check-auto-refresh"),
        btnRefreshActivities: document.getElementById("btn-refresh-activities"),
        btnClearActivities: document.getElementById("btn-clear-activities"),

        // Modal
        modalDetails: document.getElementById("modal-details"),
        btnCloseModal: document.getElementById("btn-close-modal"),
        modalMsgId: document.getElementById("modal-msg-id"),
        modalCorrId: document.getElementById("modal-corr-id"),
        modalTime: document.getElementById("modal-time"),
        modalPriority: document.getElementById("modal-priority"),
        modalProperties: document.getElementById("modal-properties"),
        modalPayload: document.getElementById("modal-payload"),

        // Toast Container
        toastContainer: document.getElementById("toast-container"),

        // Theme Toggle
        btnThemeToggle: document.getElementById("btn-theme-toggle"),
        themeToggleIcon: document.getElementById("theme-toggle-icon"),
        themeToggleText: document.getElementById("theme-toggle-text")
    };

    // Initialization
    function init() {
        setupTheme();
        loadSavedJndi();
        setupTabEvents();
        setupJndiEvents();
        setupPropertyEvents();
        setupQueueEvents();
        setupTopicEvents();
        setupActivityEvents();
        setupModalEvents();

        // Initial fetch
        fetchStatus();
        fetchActivities();
        setupAutoRefresh();
    }

    // Theme Management
    function setupTheme() {
        let savedTheme = "light";
        try {
            savedTheme = localStorage.getItem("jeus_jms_theme") || "light";
        } catch (e) {
            console.warn("Storage not accessible");
        }
        applyTheme(savedTheme);

        if (elements.btnThemeToggle) {
            elements.btnThemeToggle.addEventListener("click", function () {
                const currentTheme = document.documentElement.getAttribute("data-theme") || "light";
                const nextTheme = currentTheme === "dark" ? "light" : "dark";
                applyTheme(nextTheme);
                try {
                    localStorage.setItem("jeus_jms_theme", nextTheme);
                } catch (e) {}
            });
        }
    }

    function applyTheme(theme) {
        document.documentElement.setAttribute("data-theme", theme);
        if (elements.themeToggleIcon && elements.themeToggleText) {
            if (theme === "dark") {
                elements.themeToggleIcon.textContent = "☀️";
                elements.themeToggleText.textContent = "Light";
                elements.btnThemeToggle.setAttribute("title", "Switch to Light Mode");
            } else {
                elements.themeToggleIcon.textContent = "🌙";
                elements.themeToggleText.textContent = "Dark";
                elements.btnThemeToggle.setAttribute("title", "Switch to Dark Mode");
            }
        }
    }

    // Tab Navigation
    function setupTabEvents() {
        elements.tabButtons.forEach(function (button) {
            button.addEventListener("click", function () {
                const targetTabId = button.getAttribute("data-tab");

                elements.tabButtons.forEach(function (btn) {
                    btn.classList.remove("active");
                });
                elements.tabPanes.forEach(function (pane) {
                    pane.classList.remove("active");
                });

                button.classList.add("active");
                const targetPane = document.getElementById(targetTabId);
                if (targetPane) {
                    targetPane.classList.add("active");
                }
            });
        });
    }

    // JNDI Configuration & Storage
    function loadSavedJndi() {
        try {
            let savedCf = localStorage.getItem("jeus_cf_jndi");
            let savedDest = localStorage.getItem("jeus_dest_jndi");

            // Migrate outdated defaults if previously stored in localStorage
            if (!savedCf || savedCf === "jms/ConnectionFactory") {
                savedCf = "ConnectionFactory";
            }
            if (!savedDest || savedDest === "jms/TestQueue") {
                savedDest = "ExamplesQueue";
            }

            elements.inputCfJndi.value = savedCf;
            state.cfJndi = savedCf;
            elements.inputDestJndi.value = savedDest;
            state.destJndi = savedDest;
        } catch (e) {
            console.warn("Storage not accessible");
        }
    }

    function setupJndiEvents() {
        // Preset Selectors
        elements.presetCf.addEventListener("change", function () {
            if (elements.presetCf.value) {
                elements.inputCfJndi.value = elements.presetCf.value;
                state.cfJndi = elements.presetCf.value;
                elements.presetCf.value = "";
            }
        });

        elements.presetDest.addEventListener("change", function () {
            if (elements.presetDest.value) {
                elements.inputDestJndi.value = elements.presetDest.value;
                state.destJndi = elements.presetDest.value;
                elements.presetDest.value = "";
            }
        });

        // Save Defaults
        elements.btnSaveJndi.addEventListener("click", function () {
            state.cfJndi = elements.inputCfJndi.value.trim();
            state.destJndi = elements.inputDestJndi.value.trim();
            try {
                localStorage.setItem("jeus_cf_jndi", state.cfJndi);
                localStorage.setItem("jeus_dest_jndi", state.destJndi);
                showToast("JNDI configuration saved to browser defaults.", "success");
            } catch (e) {
                showToast("Failed to save to localStorage.", "error");
            }
        });

        // Test JNDI Lookup
        elements.btnTestJndi.addEventListener("click", function () {
            testJndiLookup();
        });
    }

    function testJndiLookup() {
        const cfJndi = elements.inputCfJndi.value.trim();
        const destJndi = elements.inputDestJndi.value.trim();

        elements.jndiResultBox.className = "result-banner hidden";
        elements.jndiResultText.textContent = "Testing JNDI lookup on JEUS server...";

        fetchApi("/api/jms/test-jndi", "POST", { cfJndi: cfJndi, destJndi: destJndi })
            .then(function (res) {
                elements.jndiResultBox.classList.remove("hidden");
                if (res.success) {
                    elements.jndiResultBox.className = "result-banner result-success";
                    let msg = "✓ SUCCESS: Connected to ConnectionFactory (" + (res.cfType || "OK") + ")";
                    if (res.destFound) {
                        msg += " and Destination (" + (res.destType || "OK") + ")";
                    }
                    elements.jndiResultText.textContent = msg;
                    showToast("JNDI verification succeeded!", "success");
                } else {
                    elements.jndiResultBox.className = "result-banner result-error";
                    elements.jndiResultText.textContent = "✗ FAILED: " + (res.error || "Lookup failed");
                    showToast("JNDI verification failed. See details above.", "error");
                }
            })
            .catch(function (err) {
                elements.jndiResultBox.classList.remove("hidden");
                elements.jndiResultBox.className = "result-banner result-error";
                elements.jndiResultText.textContent = "Network/Server Error: " + err.message;
            });
    }

    // Custom Properties Row Management
    function setupPropertyEvents() {
        elements.btnAddProperty.addEventListener("click", function () {
            addPropertyRow("", "");
        });

        elements.propertiesContainer.addEventListener("click", function (e) {
            const target = e.target;
            if (target && target.classList.contains("btn-remove-prop")) {
                const row = target.closest(".property-row");
                if (row) {
                    row.remove();
                }
            }
        });
    }

    function addPropertyRow(key, val) {
        const row = document.createElement("div");
        row.className = "property-row";

        const inputKey = document.createElement("input");
        inputKey.type = "text";
        inputKey.className = "prop-key";
        inputKey.placeholder = "Key (e.g. region)";
        inputKey.value = key;

        const inputVal = document.createElement("input");
        inputVal.type = "text";
        inputVal.className = "prop-val";
        inputVal.placeholder = "Value (e.g. KR)";
        inputVal.value = val;

        const btnRemove = document.createElement("button");
        btnRemove.type = "button";
        btnRemove.className = "btn btn-sm btn-danger btn-remove-prop";
        btnRemove.textContent = "×";

        row.appendChild(inputKey);
        row.appendChild(inputVal);
        row.appendChild(btnRemove);

        elements.propertiesContainer.appendChild(row);
    }

    function collectProperties() {
        const props = {};
        const rows = elements.propertiesContainer.querySelectorAll(".property-row");
        rows.forEach(function (row) {
            const keyEl = row.querySelector(".prop-key");
            const valEl = row.querySelector(".prop-val");
            if (keyEl && valEl) {
                const key = keyEl.value.trim();
                const val = valEl.value.trim();
                if (key.length > 0) {
                    props[key] = val;
                }
            }
        });
        return props;
    }

    // Queue Studio Operations
    function setupQueueEvents() {
        // Send Single Message
        elements.formSendQueue.addEventListener("submit", function (e) {
            e.preventDefault();
            sendQueueMessage(1);
        });

        // Batch Send 10x
        elements.btnSendBatch.addEventListener("click", function () {
            sendQueueMessage(10);
        });

        // Browse Queue
        elements.btnBrowseQueue.addEventListener("click", function () {
            browseQueue();
        });

        // Receive Message
        elements.btnReceiveQueue.addEventListener("click", function () {
            receiveQueueMessage();
        });

        // Toggle Listener
        elements.btnToggleListener.addEventListener("click", function () {
            toggleBackgroundListener();
        });

        // Clear Browse View
        elements.btnClearBrowseView.addEventListener("click", function () {
            renderBrowseTable([]);
        });
    }

    function sendQueueMessage(count) {
        const payload = elements.queuePayload.value;
        const priority = parseInt(elements.queuePriority.value, 10) || 4;
        const ttl = parseInt(elements.queueTtl.value, 10) || 0;
        const correlationId = elements.queueCorrelation.value.trim();
        const properties = collectProperties();

        const reqData = {
            cfJndi: elements.inputCfJndi.value.trim(),
            destJndi: elements.inputDestJndi.value.trim(),
            payload: payload,
            priority: priority,
            timeToLive: ttl,
            correlationId: correlationId,
            properties: properties,
            count: count
        };

        fetchApi("/api/jms/queue/send", "POST", reqData)
            .then(function (res) {
                if (res.success) {
                    showToast("Successfully sent " + res.sentCount + " message(s) to queue.", "success");
                    fetchActivities();
                } else {
                    showToast("Failed to send: " + res.error, "error");
                }
            })
            .catch(function (err) {
                showToast("Error sending message: " + err.message, "error");
            });
    }

    function browseQueue() {
        const cfJndi = encodeURIComponent(elements.inputCfJndi.value.trim());
        const destJndi = encodeURIComponent(elements.inputDestJndi.value.trim());
        const selector = encodeURIComponent(elements.browserSelector.value.trim());

        const url = "/api/jms/queue/browse?cfJndi=" + cfJndi + "&destJndi=" + destJndi + "&selector=" + selector;

        fetchApi(url, "GET")
            .then(function (res) {
                if (res.success) {
                    state.lastBrowsedMessages = res.messages || [];
                    renderBrowseTable(state.lastBrowsedMessages);
                    showToast("Browsed " + res.count + " pending message(s).", "info");
                    fetchActivities();
                } else {
                    showToast("Browse error: " + res.error, "error");
                }
            })
            .catch(function (err) {
                showToast("Browse failed: " + err.message, "error");
            });
    }

    function receiveQueueMessage() {
        const reqData = {
            cfJndi: elements.inputCfJndi.value.trim(),
            destJndi: elements.inputDestJndi.value.trim(),
            timeout: 2500,
            selector: elements.browserSelector.value.trim()
        };

        fetchApi("/api/jms/queue/receive", "POST", reqData)
            .then(function (res) {
                if (res.success) {
                    if (res.empty) {
                        showToast(res.details || "Queue is empty or timed out.", "info");
                    } else {
                        showToast("Consumed message: " + res.message.messageId, "success");
                        openMessageModal(res.message);
                    }
                    fetchActivities();
                    // Also refresh browse view if it had contents
                    if (state.lastBrowsedMessages.length > 0) {
                        browseQueue();
                    }
                } else {
                    showToast("Receive error: " + res.error, "error");
                }
            })
            .catch(function (err) {
                showToast("Receive request failed: " + err.message, "error");
            });
    }

    function toggleBackgroundListener() {
        if (state.listenerRunning) {
            // Stop listener
            fetchApi("/api/jms/listener/stop", "POST", {})
                .then(function (res) {
                    state.listenerRunning = false;
                    updateListenerUI();
                    showToast("Dynamic Background Listener stopped.", "info");
                    fetchActivities();
                })
                .catch(function (err) {
                    showToast("Failed to stop listener: " + err.message, "error");
                });
        } else {
            // Start listener
            const reqData = {
                cfJndi: elements.inputCfJndi.value.trim(),
                destJndi: elements.inputDestJndi.value.trim()
            };

            fetchApi("/api/jms/listener/start", "POST", reqData)
                .then(function (res) {
                    if (res.success) {
                        state.listenerRunning = true;
                        state.listeningDest = res.listeningDestination;
                        updateListenerUI();
                        showToast("Dynamic Background Listener active on " + res.listeningDestination, "success");
                        fetchActivities();
                    } else {
                        showToast("Could not start listener: " + res.error, "error");
                    }
                })
                .catch(function (err) {
                    showToast("Listener start failed: " + err.message, "error");
                });
        }
    }

    function updateListenerUI() {
        if (state.listenerRunning) {
            elements.listenerBadge.className = "badge badge-active";
            elements.listenerBadge.textContent = "Listener: Active (" + state.listeningDest + ")";
            elements.btnToggleListener.className = "btn btn-danger";
            elements.btnToggleListener.textContent = "⏹ Stop Listener";
        } else {
            elements.listenerBadge.className = "badge badge-inactive";
            elements.listenerBadge.textContent = "Listener: Inactive";
            elements.btnToggleListener.className = "btn btn-outline";
            elements.btnToggleListener.textContent = "▶ Start Background Listener";
        }
    }

    function renderBrowseTable(messages) {
        elements.browseTableBody.replaceChildren();
        elements.browseCountLabel.textContent = "Queue Inspection Results (" + messages.length + " messages)";

        if (messages.length === 0) {
            const tr = document.createElement("tr");
            tr.className = "empty-row";
            const td = document.createElement("td");
            td.setAttribute("colspan", "5");
            td.textContent = "No messages found in queue. Click 'Browse Messages' to query.";
            tr.appendChild(td);
            elements.browseTableBody.appendChild(tr);
            return;
        }

        messages.forEach(function (msg) {
            const tr = document.createElement("tr");

            const tdId = document.createElement("td");
            tdId.className = "mono-cell";
            tdId.textContent = truncate(msg.messageId, 24);

            const tdTime = document.createElement("td");
            tdTime.textContent = msg.timestamp;

            const tdPriority = document.createElement("td");
            tdPriority.textContent = msg.priority;

            const tdProps = document.createElement("td");
            tdProps.className = "mono-cell";
            tdProps.textContent = formatPropsSummary(msg.properties);

            const tdPayload = document.createElement("td");
            tdPayload.className = "mono-cell";
            tdPayload.textContent = truncate(msg.payload, 40);

            tr.appendChild(tdId);
            tr.appendChild(tdTime);
            tr.appendChild(tdPriority);
            tr.appendChild(tdProps);
            tr.appendChild(tdPayload);

            tr.addEventListener("click", function () {
                openMessageModal(msg);
            });

            elements.browseTableBody.appendChild(tr);
        });
    }

    // Topic Studio Operations
    function setupTopicEvents() {
        elements.formPublishTopic.addEventListener("submit", function (e) {
            e.preventDefault();

            const payload = elements.topicPayload.value;
            const priority = parseInt(elements.topicPriority.value, 10) || 4;
            const ttl = parseInt(elements.topicTtl.value, 10) || 0;
            const correlationId = elements.topicCorrelation.value.trim();

            const reqData = {
                cfJndi: elements.inputCfJndi.value.trim(),
                destJndi: elements.inputDestJndi.value.trim(),
                payload: payload,
                priority: priority,
                timeToLive: ttl,
                correlationId: correlationId
            };

            fetchApi("/api/jms/topic/publish", "POST", reqData)
                .then(function (res) {
                    if (res.success) {
                        showToast("Published broadcast message: " + res.messageId, "success");
                        fetchActivities();
                    } else {
                        showToast("Failed to publish: " + res.error, "error");
                    }
                })
                .catch(function (err) {
                    showToast("Publish error: " + err.message, "error");
                });
        });
    }

    // Activity Stream Operations
    function setupActivityEvents() {
        elements.btnRefreshActivities.addEventListener("click", function () {
            fetchActivities();
        });

        elements.btnClearActivities.addEventListener("click", function () {
            fetchApi("/api/jms/activities/clear", "POST", {})
                .then(function () {
                    fetchActivities();
                    showToast("Activity history cleared.", "info");
                });
        });

        elements.checkAutoRefresh.addEventListener("change", function () {
            setupAutoRefresh();
        });
    }

    function setupAutoRefresh() {
        if (state.autoRefreshInterval) {
            clearInterval(state.autoRefreshInterval);
            state.autoRefreshInterval = null;
        }

        if (elements.checkAutoRefresh.checked) {
            state.autoRefreshInterval = setInterval(function () {
                fetchActivities();
                fetchStatus();
            }, 2000);
        }
    }

    function fetchStatus() {
        fetchApi("/api/jms/status", "GET")
            .then(function (res) {
                if (res.success) {
                    state.listenerRunning = res.listenerRunning;
                    state.listeningDest = res.listeningDestination;
                    updateListenerUI();
                }
            })
            .catch(function () {});
    }

    function fetchActivities() {
        fetchApi("/api/jms/activities", "GET")
            .then(function (res) {
                if (res.success) {
                    renderActivityTable(res.activities || []);
                }
            })
            .catch(function () {});
    }

    function renderActivityTable(activities) {
        elements.activityTableBody.replaceChildren();

        if (activities.length === 0) {
            const tr = document.createElement("tr");
            tr.className = "empty-row";
            const td = document.createElement("td");
            td.setAttribute("colspan", "7");
            td.textContent = "No activities recorded yet. Send or receive messages to see events.";
            tr.appendChild(td);
            elements.activityTableBody.appendChild(tr);
            return;
        }

        activities.forEach(function (item) {
            const tr = document.createElement("tr");

            const tdTime = document.createElement("td");
            tdTime.textContent = item.timestamp;

            const tdAction = document.createElement("td");
            const actionSpan = document.createElement("span");
            actionSpan.className = "action-badge action-" + item.action;
            actionSpan.textContent = item.action;
            tdAction.appendChild(actionSpan);

            const tdDest = document.createElement("td");
            tdDest.className = "mono-cell";
            tdDest.textContent = item.destination;

            const tdMsgId = document.createElement("td");
            tdMsgId.className = "mono-cell";
            tdMsgId.textContent = truncate(item.messageId, 22);

            const tdProps = document.createElement("td");
            tdProps.className = "mono-cell";
            tdProps.textContent = formatPropsSummary(item.properties);

            const tdPayload = document.createElement("td");
            tdPayload.className = "mono-cell";
            tdPayload.textContent = truncate(item.payload || item.details, 30);

            const tdStatus = document.createElement("td");
            const statusSpan = document.createElement("span");
            statusSpan.className = "status-pill status-" + (item.status ? item.status.toLowerCase() : "success");
            statusSpan.textContent = item.status;
            tdStatus.appendChild(statusSpan);

            tr.appendChild(tdTime);
            tr.appendChild(tdAction);
            tr.appendChild(tdDest);
            tr.appendChild(tdMsgId);
            tr.appendChild(tdProps);
            tr.appendChild(tdPayload);
            tr.appendChild(tdStatus);

            tr.addEventListener("click", function () {
                openMessageModal(item);
            });

            elements.activityTableBody.appendChild(tr);
        });
    }

    // Modal Inspection View
    function setupModalEvents() {
        elements.btnCloseModal.addEventListener("click", function () {
            elements.modalDetails.classList.add("hidden");
        });

        elements.modalDetails.addEventListener("click", function (e) {
            if (e.target === elements.modalDetails) {
                elements.modalDetails.classList.add("hidden");
            }
        });
    }

    function openMessageModal(item) {
        elements.modalMsgId.textContent = item.messageId || "N/A";
        elements.modalCorrId.textContent = item.correlationId || "None";
        elements.modalTime.textContent = item.timestamp || "N/A";
        elements.modalPriority.textContent = (item.priority !== undefined && item.priority !== null) ? item.priority : "N/A";

        // Properties
        const props = item.properties || {};
        if (Object.keys(props).length > 0) {
            elements.modalProperties.textContent = JSON.stringify(props, null, 2);
        } else {
            elements.modalProperties.textContent = "None";
        }

        // Payload
        const payloadText = item.payload || item.details || "(Empty payload)";
        elements.modalPayload.textContent = payloadText;

        elements.modalDetails.classList.remove("hidden");
    }

    // Helpers
    const CONTEXT_PATH = (function () {
        let p = window.location.pathname || "";
        if (p.endsWith("/")) {
            p = p.slice(0, -1);
        } else if (p.includes(".")) {
            p = p.substring(0, p.lastIndexOf("/"));
        }
        return p;
    })();

    function resolveUrl(url) {
        if (url.startsWith("http://") || url.startsWith("https://")) {
            return url;
        }
        const cleanUrl = url.startsWith("/") ? url : "/" + url;
        return CONTEXT_PATH + cleanUrl;
    }

    function fetchApi(url, method, body) {
        const resolvedUrl = resolveUrl(url);
        const options = {
            method: method,
            headers: {
                "Accept": "application/json"
            }
        };

        if (body && method !== "GET") {
            options.headers["Content-Type"] = "application/json; charset=UTF-8";
            options.body = JSON.stringify(body);
        }

        return fetch(resolvedUrl, options).then(function (response) {
            if (!response.ok) {
                return response.json().then(function (errJson) {
                    throw new Error(errJson.error || ("HTTP " + response.status));
                }).catch(function (e) {
                    if (e && e.message && !e.message.startsWith("Unexpected token")) {
                        throw e;
                    }
                    throw new Error("HTTP " + response.status + ": " + response.statusText);
                });
            }
            return response.json();
        });
    }

    function showToast(message, type) {
        const toast = document.createElement("div");
        toast.className = "toast toast-" + (type || "info");

        const msgSpan = document.createElement("span");
        msgSpan.textContent = message;
        toast.appendChild(msgSpan);

        elements.toastContainer.appendChild(toast);

        setTimeout(function () {
            toast.remove();
        }, 3500);
    }

    function truncate(str, maxLen) {
        if (!str) return "N/A";
        if (str.length <= maxLen) return str;
        return str.substring(0, maxLen) + "...";
    }

    function formatPropsSummary(props) {
        if (!props) return "-";
        const keys = Object.keys(props);
        if (keys.length === 0) return "-";
        if (keys.length === 1) return keys[0] + "=" + props[keys[0]];
        return keys.length + " props (" + keys[0] + "=...)";
    }

    // Initialize on DOM ready
    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
})();
