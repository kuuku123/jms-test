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
        topicDestJndi: "ExamplesTopic",
        listenerRunning: false,
        listeningDest: "",
        activeDestinations: [],
        activeListenersMap: {},
        autoRefreshInterval: null,
        lastBrowsedMessages: [],
        lastTopicEvents: [],
        lastActivities: [],
        clearedTopicEventIds: new Set(),
        topicClearedTimestamp: null
    };

    // DOM Elements
    const elements = {
        inputCfJndi: document.getElementById("input-cf-jndi"),
        inputDestJndi: document.getElementById("input-dest-jndi"),
        inputTmJndi: document.getElementById("input-tm-jndi"),
        presetCf: document.getElementById("preset-cf"),
        presetDest: document.getElementById("preset-dest"),
        presetTm: document.getElementById("preset-tm"),
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
        btnTxCommit: document.getElementById("btn-tx-commit"),
        btnTxRollback: document.getElementById("btn-tx-rollback"),
        txBatchCount: document.getElementById("tx-batch-count"),
        presetBatchBtns: document.querySelectorAll(".btn-preset-batch"),
        txModeRadios: document.querySelectorAll('input[name="tx-mode"]'),
        browserSelector: document.getElementById("browser-selector"),
        btnBrowseQueue: document.getElementById("btn-browse-queue"),
        btnReceiveQueue: document.getElementById("btn-receive-queue"),
        btnConsumeAllQueue: document.getElementById("btn-consume-all-queue"),
        btnTableConsumeAll: document.getElementById("btn-table-consume-all"),
        tableConsumeCount: document.getElementById("table-consume-count"),
        btnToggleListener: document.getElementById("btn-toggle-listener"),
        browseCountLabel: document.getElementById("browse-count-label"),
        browseTableBody: document.getElementById("browse-table-body"),
        btnClearBrowseView: document.getElementById("btn-clear-browse-view"),

        // Topic Studio - Publisher
        topicDestJndi: document.getElementById("topic-dest-jndi"),
        presetTopicDest: document.getElementById("preset-topic-dest"),
        formPublishTopic: document.getElementById("form-publish-topic"),
        btnPublishTopicBatch: document.getElementById("btn-publish-topic-batch"),
        topicPayload: document.getElementById("topic-payload"),
        topicPriority: document.getElementById("topic-priority"),
        topicTtl: document.getElementById("topic-ttl"),
        topicCorrelation: document.getElementById("topic-correlation"),
        topicPropertiesContainer: document.getElementById("topic-properties-container"),
        btnAddTopicProperty: document.getElementById("btn-add-topic-property"),

        // Topic Studio - Subscriber
        topicSubDestJndi: document.getElementById("topic-sub-dest-jndi"),
        topicSelector: document.getElementById("topic-selector"),
        subModeNonDurable: document.getElementById("sub-mode-nondurable"),
        subModeDurable: document.getElementById("sub-mode-durable"),
        durableOptionsBox: document.getElementById("durable-options-box"),
        topicClientId: document.getElementById("topic-client-id"),
        topicSubName: document.getElementById("topic-sub-name"),
        btnTopicUnsubscribe: document.getElementById("btn-topic-unsubscribe"),
        topicListenerDesc: document.getElementById("topic-listener-desc"),
        btnToggleTopicListener: document.getElementById("btn-toggle-topic-listener"),
        topicSubscriberStatus: document.getElementById("topic-subscriber-status"),
        topicEventsCountLabel: document.getElementById("topic-events-count-label"),
        topicEventsTableBody: document.getElementById("topic-events-table-body"),
        btnClearTopicView: document.getElementById("btn-clear-topic-view"),
        btnLoadTopicHistory: document.getElementById("btn-load-topic-history"),

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
        setupTransactionEvents();
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

                // If switching to Topic Studio, sync topic destination if top bar has a topic
                if (targetTabId === "tab-topic" && elements.topicDestJndi) {
                    const topDest = elements.inputDestJndi.value.trim();
                    if (topDest && (topDest.toLowerCase().includes("topic") || topDest === "ExamplesTopic")) {
                        elements.topicDestJndi.value = topDest;
                        if (elements.topicSubDestJndi) {
                            elements.topicSubDestJndi.value = topDest;
                        }
                    }
                    updateListenerUI();
                }
            });
        });
    }

    // JNDI Configuration & Storage
    function loadSavedJndi() {
        try {
            let savedCf = localStorage.getItem("jeus_cf_jndi");
            let savedDest = localStorage.getItem("jeus_dest_jndi");
            let savedTopicDest = localStorage.getItem("jeus_topic_dest_jndi");
            let savedTm = localStorage.getItem("jeus_tm_jndi");

            if (!savedCf || savedCf === "jms/ConnectionFactory") {
                savedCf = "ConnectionFactory";
            }
            if (!savedDest || savedDest === "jms/TestQueue") {
                savedDest = "ExamplesQueue";
            }
            if (!savedTopicDest) {
                savedTopicDest = "ExamplesTopic";
            }
            if (!savedTm) {
                savedTm = "java:/TransactionManager";
            }

            elements.inputCfJndi.value = savedCf;
            state.cfJndi = savedCf;
            elements.inputDestJndi.value = savedDest;
            state.destJndi = savedDest;
            if (elements.inputTmJndi) {
                elements.inputTmJndi.value = savedTm;
            }
            state.tmJndi = savedTm;

            if (elements.topicDestJndi) {
                elements.topicDestJndi.value = savedTopicDest;
            }
            if (elements.topicSubDestJndi) {
                elements.topicSubDestJndi.value = savedTopicDest;
            }
            state.topicDestJndi = savedTopicDest;
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

                // If a topic preset was selected, also update Topic Studio inputs
                if (elements.presetDest.value.toLowerCase().includes("topic")) {
                    if (elements.topicDestJndi) elements.topicDestJndi.value = elements.presetDest.value;
                    if (elements.topicSubDestJndi) elements.topicSubDestJndi.value = elements.presetDest.value;
                }

                elements.presetDest.value = "";
                updateListenerUI();
            }
        });

        if (elements.presetTm) {
            elements.presetTm.addEventListener("change", function () {
                if (elements.presetTm.value) {
                    elements.inputTmJndi.value = elements.presetTm.value;
                    state.tmJndi = elements.presetTm.value;
                    elements.presetTm.value = "";
                }
            });
        }

        // Save Defaults
        elements.btnSaveJndi.addEventListener("click", function () {
            state.cfJndi = elements.inputCfJndi.value.trim();
            state.destJndi = elements.inputDestJndi.value.trim();
            const topicDest = elements.topicDestJndi ? elements.topicDestJndi.value.trim() : "ExamplesTopic";
            const tmJndi = elements.inputTmJndi ? elements.inputTmJndi.value.trim() : "java:/TransactionManager";
            state.tmJndi = tmJndi;
            try {
                localStorage.setItem("jeus_cf_jndi", state.cfJndi);
                localStorage.setItem("jeus_dest_jndi", state.destJndi);
                localStorage.setItem("jeus_topic_dest_jndi", topicDest);
                localStorage.setItem("jeus_tm_jndi", state.tmJndi);
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
        const tmJndi = elements.inputTmJndi ? elements.inputTmJndi.value.trim() : "java:/TransactionManager";

        elements.jndiResultBox.className = "result-banner hidden";
        elements.jndiResultText.textContent = "Testing JNDI lookup on JEUS server...";

        fetchApi("/api/jms/test-jndi", "POST", { cfJndi: cfJndi, destJndi: destJndi, tmJndi: tmJndi })
            .then(function (res) {
                elements.jndiResultBox.classList.remove("hidden");
                if (res.success) {
                    elements.jndiResultBox.className = "result-banner result-success";
                    let msg = "✓ SUCCESS: Connected to ConnectionFactory (" + (res.cfType || "OK") + ")";
                    if (res.destFound) {
                        msg += " and Destination (" + (res.destType || "OK") + ")";
                    }
                    if (res.tmFound) {
                        msg += " and TransactionManager (" + (res.tmType || "OK") + ", " + (res.tmStatus || "Ready") + ")";
                    } else if (res.tmError) {
                        msg += " [TM Notice: " + res.tmError + "]";
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
        // Queue properties
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

        // Topic properties
        if (elements.btnAddTopicProperty) {
            elements.btnAddTopicProperty.addEventListener("click", function () {
                addTopicPropertyRow("", "");
            });
        }

        if (elements.topicPropertiesContainer) {
            elements.topicPropertiesContainer.addEventListener("click", function (e) {
                const target = e.target;
                if (target && target.classList.contains("btn-remove-prop")) {
                    const row = target.closest(".property-row");
                    if (row) {
                        row.remove();
                    }
                }
            });

            // Default initial filterable properties for Topic Studio
            addTopicPropertyRow("eventType", "PRICE_UPDATE");
            addTopicPropertyRow("symbol", "TMAX");
        }
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

    function addTopicPropertyRow(key, val) {
        if (!elements.topicPropertiesContainer) return;
        const row = document.createElement("div");
        row.className = "property-row";

        const inputKey = document.createElement("input");
        inputKey.type = "text";
        inputKey.className = "prop-key";
        inputKey.placeholder = "Key (e.g. eventType)";
        inputKey.value = key;

        const inputVal = document.createElement("input");
        inputVal.type = "text";
        inputVal.className = "prop-val";
        inputVal.placeholder = "Value (e.g. PRICE_UPDATE)";
        inputVal.value = val;

        const btnRemove = document.createElement("button");
        btnRemove.type = "button";
        btnRemove.className = "btn btn-sm btn-danger btn-remove-prop";
        btnRemove.textContent = "×";

        row.appendChild(inputKey);
        row.appendChild(inputVal);
        row.appendChild(btnRemove);

        elements.topicPropertiesContainer.appendChild(row);
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

    function collectTopicProperties() {
        if (!elements.topicPropertiesContainer) return {};
        const props = {};
        const rows = elements.topicPropertiesContainer.querySelectorAll(".property-row");
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

        // Receive Single Message
        elements.btnReceiveQueue.addEventListener("click", function () {
            receiveQueueMessage();
        });

        // Consume All Messages (Drain)
        if (elements.btnConsumeAllQueue) {
            elements.btnConsumeAllQueue.addEventListener("click", function () {
                consumeAllQueueMessages();
            });
        }

        // Table Header Consume All Button
        if (elements.btnTableConsumeAll) {
            elements.btnTableConsumeAll.addEventListener("click", function () {
                consumeAllQueueMessages();
            });
        }

        // Toggle Listener
        elements.btnToggleListener.addEventListener("click", function () {
            toggleBackgroundListener();
        });

        // Clear Browse View
        elements.btnClearBrowseView.addEventListener("click", function () {
            state.lastBrowsedMessages = [];
            renderBrowseTable([]);
            if (elements.btnTableConsumeAll) {
                elements.btnTableConsumeAll.style.display = "none";
            }
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

    // Transaction Lab Event Setup
    function setupTransactionEvents() {
        if (elements.btnTxCommit) {
            elements.btnTxCommit.addEventListener("click", function () {
                sendTransactionalQueueMessage(false);
            });
        }

        if (elements.btnTxRollback) {
            elements.btnTxRollback.addEventListener("click", function () {
                sendTransactionalQueueMessage(true);
            });
        }

        if (elements.presetBatchBtns) {
            elements.presetBatchBtns.forEach(function (btn) {
                btn.addEventListener("click", function () {
                    elements.presetBatchBtns.forEach(function (b) { b.classList.remove("active"); });
                    btn.classList.add("active");
                    const count = parseInt(btn.getAttribute("data-count"), 10) || 10;
                    if (elements.txBatchCount) {
                        elements.txBatchCount.value = count;
                    }
                });
            });
        }

        if (elements.txModeRadios) {
            elements.txModeRadios.forEach(function (radio) {
                radio.addEventListener("change", function () {
                    document.querySelectorAll(".tx-radio-pill").forEach(function (pill) {
                        pill.classList.remove("active");
                    });
                    const parentPill = radio.closest(".tx-radio-pill");
                    if (parentPill) parentPill.classList.add("active");
                });
            });
        }
    }

    function sendTransactionalQueueMessage(simulateRollback) {
        const payload = elements.queuePayload.value;
        const priority = parseInt(elements.queuePriority.value, 10) || 4;
        const ttl = parseInt(elements.queueTtl.value, 10) || 0;
        const correlationId = elements.queueCorrelation.value.trim();
        const properties = collectProperties();
        const count = elements.txBatchCount ? (parseInt(elements.txBatchCount.value, 10) || 10) : 10;

        let selectedTxMode = "JTA";
        const checkedRadio = document.querySelector('input[name="tx-mode"]:checked');
        if (checkedRadio) {
            selectedTxMode = checkedRadio.value;
        }

        const tmJndi = elements.inputTmJndi ? elements.inputTmJndi.value.trim() : "java:/TransactionManager";
        let cfJndi = elements.inputCfJndi.value.trim();
        // If JTA mode is selected and CF is standard ConnectionFactory, auto-use XAConnectionFactory for 2PC enlistment
        if (selectedTxMode === "JTA" && (cfJndi === "ConnectionFactory" || !cfJndi)) {
            cfJndi = "XAConnectionFactory";
        }

        const reqData = {
            cfJndi: cfJndi,
            destJndi: elements.inputDestJndi.value.trim(),
            payload: payload,
            priority: priority,
            timeToLive: ttl,
            correlationId: correlationId,
            properties: properties,
            count: count,
            txType: selectedTxMode,
            simulateRollback: simulateRollback,
            tmJndi: tmJndi
        };

        const actionText = simulateRollback ? "Simulating Rollback" : "Committing Batch";
        showToast(actionText + " (" + count + " msgs via " + selectedTxMode + ")...", "info");

        fetchApi("/api/jms/queue/send-transactional", "POST", reqData)
            .then(function (res) {
                if (res.success) {
                    if (simulateRollback) {
                        showToast(res.message || ("Rollback succeeded: 0 messages persisted."), "warning");
                    } else {
                        showToast(res.message || ("Committed " + (res.sentCount || count) + " messages atomically!"), "success");
                    }
                    fetchActivities();
                    // Auto-browse to let user immediately inspect the actual queue messages
                    browseQueue();
                } else {
                    showToast("Transaction failed: " + (res.error || res.message || "Unknown error"), "error");
                    fetchActivities();
                }
            })
            .catch(function (err) {
                showToast("Transaction error: " + err.message, "error");
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

    function consumeAllQueueMessages() {
        const destJndi = elements.inputDestJndi.value.trim();
        const cfJndi = elements.inputCfJndi.value.trim();
        const selector = elements.browserSelector.value.trim();

        const btnMain = elements.btnConsumeAllQueue;
        const btnTable = elements.btnTableConsumeAll;
        const origMainText = btnMain ? btnMain.innerHTML : "";
        const origTableText = btnTable ? btnTable.innerHTML : "";

        if (btnMain) {
            btnMain.disabled = true;
            btnMain.innerHTML = "⏳ Consuming...";
        }
        if (btnTable) {
            btnTable.disabled = true;
            btnTable.innerHTML = "⏳ Consuming...";
        }

        const reqData = {
            cfJndi: cfJndi,
            destJndi: destJndi,
            selector: selector,
            maxCount: 1000,
            timeout: 1500
        };

        showToast("Consuming all messages from " + destJndi + "...", "info");

        fetchApi("/api/jms/queue/receive-all", "POST", reqData)
            .then(function (res) {
                if (res.success) {
                    if (res.empty || res.count === 0) {
                        showToast(res.details || ("Queue " + destJndi + " is empty (0 messages consumed)."), "info");
                        browseQueue();
                    } else {
                        showToast("Successfully consumed all " + res.count + " messages from " + destJndi + "!", "success");
                        renderConsumedTable(res.messages, destJndi);
                    }
                    fetchActivities();
                } else {
                    showToast("Failed to consume messages: " + (res.error || "Unknown error"), "error");
                }
            })
            .catch(function (err) {
                showToast("Consume request failed: " + err.message, "error");
            })
            .finally(function () {
                if (btnMain) {
                    btnMain.disabled = false;
                    btnMain.innerHTML = origMainText;
                }
                if (btnTable) {
                    btnTable.disabled = false;
                    btnTable.innerHTML = origTableText;
                }
            });
    }

    function toggleBackgroundListener() {
        const destJndi = elements.inputDestJndi.value.trim();
        const cfJndi = elements.inputCfJndi.value.trim();
        const isListening = state.activeDestinations && state.activeDestinations.includes(destJndi);

        if (isListening) {
            fetchApi("/api/jms/listener/stop", "POST", { destJndi: destJndi })
                .then(function (res) {
                    state.activeDestinations = res.activeDestinations || [];
                    state.listenerRunning = res.listenerRunning;
                    updateListenerUI();
                    showToast("Stopped background listener on " + destJndi, "info");
                    fetchActivities();
                })
                .catch(function (err) {
                    showToast("Failed to stop listener: " + err.message, "error");
                });
        } else {
            const reqData = {
                cfJndi: cfJndi,
                destJndi: destJndi,
                selector: elements.browserSelector.value.trim()
            };

            fetchApi("/api/jms/listener/start", "POST", reqData)
                .then(function (res) {
                    if (res.success) {
                        state.activeDestinations = res.activeDestinations || [res.listeningDestination];
                        state.listenerRunning = true;
                        updateListenerUI();
                        showToast("Background listener active on " + res.listeningDestination, "success");
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

    function toggleTopicListener() {
        const topicDest = (elements.topicSubDestJndi ? elements.topicSubDestJndi.value.trim() : "") || "ExamplesTopic";
        const cfJndi = elements.inputCfJndi.value.trim();
        const isListening = state.activeDestinations && state.activeDestinations.includes(topicDest);

        if (isListening) {
            fetchApi("/api/jms/listener/stop", "POST", { destJndi: topicDest })
                .then(function (res) {
                    state.activeDestinations = res.activeDestinations || [];
                    state.listenerRunning = res.listenerRunning;
                    if (state.activeListenersMap && state.activeListenersMap[topicDest]) {
                        delete state.activeListenersMap[topicDest];
                    }
                    updateListenerUI();
                    showToast("Stopped topic subscriber on " + topicDest, "info");
                    fetchActivities();
                })
                .catch(function (err) {
                    showToast("Failed to stop topic subscriber: " + err.message, "error");
                });
        } else {
            const isDurable = elements.subModeDurable && elements.subModeDurable.checked;
            const clientId = elements.topicClientId ? elements.topicClientId.value.trim() : "JmsStudioClient-01";
            const subscriptionName = elements.topicSubName ? elements.topicSubName.value.trim() : "TopicSub-01";

            if (isDurable) {
                if (!clientId) {
                    showToast("ClientID is required for durable subscription.", "error");
                    return;
                }
                if (!subscriptionName) {
                    showToast("Subscription Name is required for durable subscription.", "error");
                    return;
                }
            }

            const reqData = {
                cfJndi: cfJndi,
                destJndi: topicDest,
                selector: elements.topicSelector ? elements.topicSelector.value.trim() : "",
                durable: isDurable,
                clientId: clientId,
                subscriptionName: subscriptionName
            };

            fetchApi("/api/jms/listener/start", "POST", reqData)
                .then(function (res) {
                    if (res.success) {
                        state.activeDestinations = res.activeDestinations || [res.listeningDestination];
                        state.listenerRunning = true;
                        fetchStatus();
                        const modeMsg = isDurable ? " (Durable: " + subscriptionName + ")" : "";
                        showToast("Topic subscriber active on " + res.listeningDestination + modeMsg, "success");
                        fetchActivities();
                    } else {
                        showToast("Could not start topic subscriber: " + res.error, "error");
                    }
                })
                .catch(function (err) {
                    showToast("Topic subscriber start failed: " + err.message, "error");
                });
        }
    }

    function updateListenerUI() {
        const activeList = state.activeDestinations || [];
        const queueDest = elements.inputDestJndi.value.trim();
        const topicDest = elements.topicSubDestJndi ? elements.topicSubDestJndi.value.trim() : "ExamplesTopic";

        // Global Header Badge
        if (activeList.length > 0) {
            elements.listenerBadge.className = "badge badge-active";
            elements.listenerBadge.textContent = "Listeners: " + activeList.length + " Active (" + activeList.join(", ") + ")";
        } else {
            elements.listenerBadge.className = "badge badge-inactive";
            elements.listenerBadge.textContent = "Listener: Inactive";
        }

        // Queue Listener Button
        const isQueueActive = activeList.includes(queueDest);
        if (isQueueActive) {
            elements.btnToggleListener.className = "btn btn-danger";
            elements.btnToggleListener.textContent = "⏹ Stop Listener";
        } else {
            elements.btnToggleListener.className = "btn btn-outline";
            elements.btnToggleListener.textContent = "▶ Start Background Listener";
        }

        // Topic Subscriber Button & Status
        if (elements.btnToggleTopicListener && elements.topicSubscriberStatus) {
            const isTopicActive = activeList.includes(topicDest);
            if (isTopicActive) {
                elements.btnToggleTopicListener.className = "btn btn-danger";
                elements.btnToggleTopicListener.textContent = "⏹ Stop Topic Subscriber";
                elements.topicSubscriberStatus.className = "subscriber-live-tag active";

                const listenerMeta = state.activeListenersMap && state.activeListenersMap[topicDest];
                if (listenerMeta && listenerMeta.durable) {
                    const subInfo = listenerMeta.subscriptionName ? " [DURABLE: " + listenerMeta.subscriptionName + "]" : " [DURABLE]";
                    elements.topicSubscriberStatus.textContent = "● ACTIVE (" + topicDest + subInfo + ")";
                } else {
                    elements.topicSubscriberStatus.textContent = "● ACTIVE (" + topicDest + ")";
                }

                // Lock inputs while active
                if (elements.subModeNonDurable) elements.subModeNonDurable.disabled = true;
                if (elements.subModeDurable) elements.subModeDurable.disabled = true;
                if (elements.topicClientId) elements.topicClientId.disabled = true;
                if (elements.topicSubName) elements.topicSubName.disabled = true;
            } else {
                elements.btnToggleTopicListener.className = "btn btn-outline";
                elements.btnToggleTopicListener.textContent = "▶ Start Topic Subscriber";
                elements.topicSubscriberStatus.className = "subscriber-live-tag inactive";
                elements.topicSubscriberStatus.textContent = "○ INACTIVE";

                // Re-enable inputs
                if (elements.subModeNonDurable) elements.subModeNonDurable.disabled = false;
                if (elements.subModeDurable) elements.subModeDurable.disabled = false;
                if (elements.topicClientId) elements.topicClientId.disabled = false;
                if (elements.topicSubName) elements.topicSubName.disabled = false;
            }
        }
    }

    function renderBrowseTable(messages) {
        elements.browseTableBody.replaceChildren();
        elements.browseCountLabel.textContent = "Queue Inspection Results (" + messages.length + " messages)";

        if (elements.btnTableConsumeAll) {
            if (messages.length > 0) {
                elements.btnTableConsumeAll.style.display = "inline-flex";
                if (elements.tableConsumeCount) {
                    elements.tableConsumeCount.textContent = messages.length;
                }
            } else {
                elements.btnTableConsumeAll.style.display = "none";
            }
        }

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

    function renderConsumedTable(messages, destJndi) {
        elements.browseTableBody.replaceChildren();
        elements.browseCountLabel.textContent = "Consumed " + messages.length + " Message(s) from " + destJndi + " (Queue Drained)";
        if (elements.btnTableConsumeAll) {
            elements.btnTableConsumeAll.style.display = "none";
        }
        state.lastBrowsedMessages = [];

        messages.forEach(function (msg) {
            const tr = document.createElement("tr");
            tr.className = "consumed-row";

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
        if (elements.presetTopicDest) {
            elements.presetTopicDest.addEventListener("change", function () {
                if (elements.presetTopicDest.value) {
                    elements.topicDestJndi.value = elements.presetTopicDest.value;
                    if (elements.topicSubDestJndi) {
                        elements.topicSubDestJndi.value = elements.presetTopicDest.value;
                    }
                    elements.presetTopicDest.value = "";
                    updateListenerUI();
                }
            });
        }

        if (elements.topicDestJndi) {
            elements.topicDestJndi.addEventListener("input", function () {
                if (elements.topicSubDestJndi) {
                    elements.topicSubDestJndi.value = elements.topicDestJndi.value;
                }
            });
        }

        if (elements.topicSubDestJndi) {
            elements.topicSubDestJndi.addEventListener("input", function () {
                updateListenerUI();
            });
        }

        // Publish Single
        elements.formPublishTopic.addEventListener("submit", function (e) {
            e.preventDefault();
            publishTopicMessage(1);
        });

        // Batch Publish 5x
        if (elements.btnPublishTopicBatch) {
            elements.btnPublishTopicBatch.addEventListener("click", function () {
                publishTopicMessage(5);
            });
        }

        // Toggle Topic Listener
        if (elements.btnToggleTopicListener) {
            elements.btnToggleTopicListener.addEventListener("click", function () {
                toggleTopicListener();
            });
        }

        // Subscription Mode Radio Toggle (Non-Durable vs Durable)
        function updateDurableModeVisibility() {
            const isDurable = elements.subModeDurable && elements.subModeDurable.checked;
            if (elements.durableOptionsBox) {
                if (isDurable) {
                    elements.durableOptionsBox.classList.remove("hidden");
                } else {
                    elements.durableOptionsBox.classList.add("hidden");
                }
            }
            if (elements.topicListenerDesc) {
                if (isDurable) {
                    elements.topicListenerDesc.textContent = "Registers a durable subscription in JEUS with ClientID & Sub Name. JEUS buffers broadcast messages even when subscriber is offline.";
                } else {
                    elements.topicListenerDesc.textContent = "Attaches a live JMS MessageListener to receive topic broadcast events in real-time.";
                }
            }
        }

        if (elements.subModeNonDurable) {
            elements.subModeNonDurable.addEventListener("change", updateDurableModeVisibility);
        }
        if (elements.subModeDurable) {
            elements.subModeDurable.addEventListener("change", updateDurableModeVisibility);
        }

        // Unsubscribe Button
        if (elements.btnTopicUnsubscribe) {
            elements.btnTopicUnsubscribe.addEventListener("click", function () {
                const cfJndi = elements.inputCfJndi.value.trim();
                const clientId = elements.topicClientId ? elements.topicClientId.value.trim() : "JmsStudioClient-01";
                const subName = elements.topicSubName ? elements.topicSubName.value.trim() : "TopicSub-01";

                if (!clientId || !subName) {
                    showToast("Please provide both ClientID and Subscription Name to unsubscribe.", "error");
                    return;
                }

                if (!confirm("Are you sure you want to unsubscribe '" + subName + "' (ClientID: '" + clientId + "')? JEUS will discard any buffered messages for this subscriber.")) {
                    return;
                }

                fetchApi("/api/jms/topic/unsubscribe", "POST", {
                    cfJndi: cfJndi,
                    clientId: clientId,
                    subscriptionName: subName
                })
                    .then(function (res) {
                        if (res.success) {
                            showToast(res.message || "Durable subscription unsubscribed successfully.", "success");
                            fetchStatus();
                            fetchActivities();
                        } else {
                            showToast("Failed to unsubscribe: " + res.error, "error");
                        }
                    })
                    .catch(function (err) {
                        showToast("Unsubscribe error: " + err.message, "error");
                    });
            });
        }

        // Clear Topic Events View
        if (elements.btnClearTopicView) {
            elements.btnClearTopicView.addEventListener("click", function () {
                if (!state.clearedTopicEventIds) {
                    state.clearedTopicEventIds = new Set();
                }
                const currentActivities = state.lastActivities || [];
                currentActivities.forEach(function (act) {
                    state.clearedTopicEventIds.add(act.id);
                    if (act.timestamp && (!state.topicClearedTimestamp || act.timestamp > state.topicClearedTimestamp)) {
                        state.topicClearedTimestamp = act.timestamp;
                    }
                });

                const now = new Date();
                const pad = function (n, z) { z = z || 2; return ('00' + n).slice(-z); };
                const nowStr = now.getFullYear() + '-' +
                    pad(now.getMonth() + 1) + '-' +
                    pad(now.getDate()) + ' ' +
                    pad(now.getHours()) + ':' +
                    pad(now.getMinutes()) + ':' +
                    pad(now.getSeconds()) + '.' +
                    pad(now.getMilliseconds(), 3);
                if (!state.topicClearedTimestamp || nowStr > state.topicClearedTimestamp) {
                    state.topicClearedTimestamp = nowStr;
                }

                renderTopicEventsTable([]);
                showToast("Cleared Topic broadcast events view.", "info");
            });
        }

        // Reload Topic Events from History
        if (elements.btnLoadTopicHistory) {
            elements.btnLoadTopicHistory.addEventListener("click", function () {
                if (state.clearedTopicEventIds) {
                    state.clearedTopicEventIds.clear();
                }
                state.topicClearedTimestamp = null;
                updateTopicEventsFromActivities(state.lastActivities || []);
                showToast("Loaded broadcast events from history.", "info");
            });
        }
    }

    function publishTopicMessage(count) {
        const payload = elements.topicPayload.value;
        const priority = parseInt(elements.topicPriority.value, 10) || 4;
        const ttl = parseInt(elements.topicTtl.value, 10) || 0;
        const correlationId = elements.topicCorrelation.value.trim();
        const topicDest = (elements.topicDestJndi ? elements.topicDestJndi.value.trim() : "") || elements.inputDestJndi.value.trim() || "ExamplesTopic";
        const properties = collectTopicProperties();

        const reqData = {
            cfJndi: elements.inputCfJndi.value.trim(),
            destJndi: topicDest,
            payload: payload,
            priority: priority,
            timeToLive: ttl,
            correlationId: correlationId,
            properties: properties,
            count: count
        };

        fetchApi("/api/jms/topic/publish", "POST", reqData)
            .then(function (res) {
                if (res.success) {
                    if (count > 1) {
                        showToast("Successfully broadcast " + res.publishedCount + " messages to topic.", "success");
                    } else {
                        showToast("Published broadcast message: " + (res.messageId || "OK"), "success");
                    }
                    fetchActivities();
                } else {
                    showToast("Failed to publish: " + res.error, "error");
                }
            })
            .catch(function (err) {
                showToast("Publish error: " + err.message, "error");
            });
    }

    function updateTopicEventsFromActivities(activities) {
        if (!elements.topicEventsTableBody) return;

        const topicSubDest = (elements.topicSubDestJndi ? elements.topicSubDestJndi.value.trim() : "ExamplesTopic");
        const topicEvents = activities.filter(function (act) {
            if (act.action !== "TOPIC_RECV") return false;
            if (topicSubDest && act.destination !== topicSubDest) return false;
            if (state.clearedTopicEventIds && state.clearedTopicEventIds.has(act.id)) return false;
            if (state.topicClearedTimestamp && act.timestamp <= state.topicClearedTimestamp) return false;
            return true;
        });

        renderTopicEventsTable(topicEvents);
    }

    function renderTopicEventsTable(messages) {
        if (!elements.topicEventsTableBody) return;
        elements.topicEventsTableBody.replaceChildren();
        if (elements.topicEventsCountLabel) {
            elements.topicEventsCountLabel.textContent = "Received Broadcast Events (" + messages.length + " events)";
        }

        if (messages.length === 0) {
            const tr = document.createElement("tr");
            tr.className = "empty-row";
            const td = document.createElement("td");
            td.setAttribute("colspan", "5");
            td.textContent = "No broadcast events received yet. Start the Topic Subscriber above and publish messages!";
            tr.appendChild(td);
            elements.topicEventsTableBody.appendChild(tr);
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

            elements.topicEventsTableBody.appendChild(tr);
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
                    state.lastActivities = [];
                    if (state.clearedTopicEventIds) state.clearedTopicEventIds.clear();
                    state.topicClearedTimestamp = null;
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
                    state.activeDestinations = res.activeDestinations || [];
                    state.activeListenersMap = res.activeListenersMap || {};
                    updateListenerUI();
                }
            })
            .catch(function () {});
    }

    function fetchActivities() {
        fetchApi("/api/jms/activities", "GET")
            .then(function (res) {
                if (res.success) {
                    const activities = res.activities || [];
                    state.lastActivities = activities;
                    renderActivityTable(activities);
                    updateTopicEventsFromActivities(activities);
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
