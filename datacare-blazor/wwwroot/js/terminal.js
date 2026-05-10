// ── Remove focus outline from headings ────────────────────────────────────
document.addEventListener('DOMContentLoaded', function () {
    function blurNonInteractive() {
        var el = document.activeElement;
        if (el && el !== document.body) {
            var tag = el.tagName.toLowerCase();
            if (tag !== 'input' && tag !== 'select' && tag !== 'textarea' && tag !== 'button' && tag !== 'a') {
                el.blur();
            }
        }
    }
    setTimeout(blurNonInteractive, 50);
    setTimeout(blurNonInteractive, 200);
    setTimeout(blurNonInteractive, 500);
    document.addEventListener('enhancedload', function () {
        setTimeout(blurNonInteractive, 50);
        setTimeout(blurNonInteractive, 200);
    });
});

// ── Terminal scroll ────────────────────────────────────────────────────────
window.datacareTerminal = {
    scrollToBottom: function (id) {
        var el = document.getElementById(id);
        if (el) el.scrollTop = el.scrollHeight;
    }
};

// ── Chart tooltip ──────────────────────────────────────────────────────────
window.datacareChart = (function () {
    var tip = null;
    function getTip() { return tip || (tip = document.getElementById('chart-tooltip')); }
    function pos(el, e) {
        var x = e.clientX + 14, y = e.clientY - 38;
        var w = el.offsetWidth || 160;
        if (x + w > window.innerWidth - 8) x = e.clientX - w - 14;
        el.style.left = x + 'px'; el.style.top = y + 'px';
    }
    return {
        show: function (e, label) {
            var el = getTip(); if (!el) return;
            el.textContent = label; el.style.display = 'block'; pos(el, e);
        },
        move: function (e) { var el = getTip(); if (el && el.style.display !== 'none') pos(el, e); },
        hide: function () { var el = getTip(); if (el) el.style.display = 'none'; }
    };
})();

// ── Export: JSON, CSV, PDF ────────────────────────────────────────────────
window.datacareExport = {
    downloadBase64: function (filename, mime, b64) {
        var a = document.createElement('a');
        a.href = 'data:' + mime + ';base64,' + b64;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        setTimeout(function () { document.body.removeChild(a); }, 200);
    },

    // PDF via html2canvas + jsPDF — cattura il DOM renderizzato come immagine
    downloadPdf: async function () {
        var dash = document.querySelector('.dashboard-page');
        if (!dash) { alert('Dashboard non trovata.'); return; }

        // Nascondi controlli e tooltip
        var ctrl = dash.querySelector('.dash-controls');
        var tip = document.getElementById('chart-tooltip');
        if (ctrl) ctrl.style.visibility = 'hidden';
        if (tip) tip.style.display = 'none';

        try {
            // Aspetta 800ms per essere sicuri che i tile CartoDB siano caricati
            await new Promise(function (r) { setTimeout(r, 800); });

            var fullH = dash.scrollHeight;
            var fullW = dash.scrollWidth;

            var canvas = await html2canvas(dash, {
                backgroundColor: '#080c18',
                scale: 2,
                useCORS: true,
                allowTaint: true,
                logging: false,
                width: fullW,
                height: fullH,
                windowWidth: fullW,
                windowHeight: fullH,
                scrollX: 0,
                scrollY: -window.scrollY,
                ignoreElements: function (el) {
                    return el.id === 'chart-tooltip';
                }
            });

            var imgData = canvas.toDataURL('image/jpeg', 0.92);

            var { jsPDF } = window.jspdf;
            var pdfW = 297;
            var pdfH = 210;
            var pdf = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' });

            var imgPxW = canvas.width;
            var imgPxH = canvas.height;
            var ratio = pdfW / imgPxW;
            var rendH = imgPxH * ratio;
            var pageH = pdfH - 10;

            if (rendH <= pageH) {
                pdf.addImage(imgData, 'JPEG', 0, 5, pdfW, rendH);
            } else {
                var sliceH = Math.floor(pageH / ratio);
                var pagesPx = Math.ceil(imgPxH / sliceH);
                for (var p = 0; p < pagesPx; p++) {
                    if (p > 0) pdf.addPage();
                    var sliceCanvas = document.createElement('canvas');
                    sliceCanvas.width = imgPxW;
                    sliceCanvas.height = Math.min(sliceH, imgPxH - p * sliceH);
                    var ctx = sliceCanvas.getContext('2d');
                    ctx.drawImage(canvas, 0, p * sliceH, imgPxW, sliceCanvas.height,
                        0, 0, imgPxW, sliceCanvas.height);
                    var sliceData = sliceCanvas.toDataURL('image/jpeg', 0.92);
                    pdf.addImage(sliceData, 'JPEG', 0, 5, pdfW, sliceCanvas.height * ratio);
                }
            }

            var dept = 'all';
            try { var sel = document.querySelector('.filter-select'); if (sel && sel.value) dept = sel.value; } catch (e) { }
            var now = new Date();
            var ds = now.getFullYear() + '' + String(now.getMonth() + 1).padStart(2, '0') + String(now.getDate()).padStart(2, '0');
            pdf.save('datacare_' + dept + '_' + ds + '.pdf');

        } catch (err) {
            console.error('PDF error:', err);
            alert('Errore nella generazione del PDF: ' + err.message);
        } finally {
            if (ctrl) ctrl.style.visibility = '';
            if (tip) tip.style.display = '';
        }
    }
};

// ── World bubble map — Leaflet.js ────────────────────────────────────────
window.datacareMap = (function () {

    var _mapInstances = {};

    var CENTROIDS = {
        'Italy': [41.87, 12.57], 'Germany': [51.17, 10.45], 'France': [46.23, 2.21],
        'Spain': [40.46, -3.75], 'United Kingdom': [55.38, -3.44], 'UK': [55.38, -3.44],
        'Netherlands': [52.13, 5.29], 'Belgium': [50.50, 4.47], 'Switzerland': [46.82, 8.23],
        'Austria': [47.52, 14.55], 'Portugal': [39.40, -8.22], 'Poland': [51.92, 19.14],
        'Czech Republic': [49.82, 15.47], 'Czechia': [49.82, 15.47],
        'Hungary': [47.16, 19.50], 'Romania': [45.94, 24.97], 'Sweden': [60.13, 18.64],
        'Norway': [60.47, 8.47], 'Denmark': [56.26, 9.50], 'Finland': [61.92, 25.73],
        'Greece': [39.07, 21.82], 'Croatia': [45.10, 15.20], 'Serbia': [44.02, 21.01],
        'Slovakia': [48.67, 19.70], 'Bulgaria': [42.73, 25.49], 'Luxembourg': [49.82, 6.13],
        'Ireland': [53.41, -8.24], 'Turkey': [38.96, 35.24], 'Russia': [61.52, 105.32],
        'Ukraine': [48.38, 31.17], 'United States': [37.09, -101.3], 'USA': [37.09, -101.3],
        'Canada': [56.13, -96.80], 'Mexico': [23.63, -102.55], 'Brazil': [-14.24, -51.93],
        'Argentina': [-38.42, -63.62], 'Chile': [-35.68, -71.54], 'Colombia': [4.57, -74.30],
        'Peru': [-9.19, -75.02], 'China': [35.86, 104.19], 'Japan': [36.20, 138.25],
        'South Korea': [35.91, 127.77], 'India': [20.59, 78.96], 'Indonesia': [-0.79, 113.92],
        'Australia': [-25.27, 133.78], 'New Zealand': [-40.90, 174.89],
        'South Africa': [-28.48, 25.08], 'Egypt': [26.82, 30.80], 'Nigeria': [9.08, 8.68],
        'Kenya': [-0.02, 37.91], 'Morocco': [31.79, -7.09],
        'United Arab Emirates': [23.42, 53.85], 'UAE': [23.42, 53.85],
        'Saudi Arabia': [23.88, 45.08], 'Israel': [31.05, 34.85],
        'Qatar': [25.35, 51.18], 'Singapore': [1.35, 103.82], 'Hong Kong': [22.32, 114.17],
        'Taiwan': [23.70, 120.96], 'Thailand': [15.87, 100.99], 'Vietnam': [14.06, 108.28],
        'Malaysia': [4.21, 109.70], 'Philippines': [12.88, 122.88],
        'Pakistan': [30.38, 69.34], 'Bangladesh': [23.68, 90.36]
    };

    function resolve(name) {
        if (CENTROIDS[name]) return CENTROIDS[name];
        var lo = name.toLowerCase();
        for (var k in CENTROIDS) if (k.toLowerCase() === lo) return CENTROIDS[k];
        for (var k in CENTROIDS) {
            var kl = k.toLowerCase();
            if (lo.indexOf(kl) !== -1 || kl.indexOf(lo) !== -1) return CENTROIDS[k];
        }
        return null;
    }

    function doInit(containerId, points) {
        if (_mapInstances[containerId]) {
            _mapInstances[containerId].remove();
            delete _mapInstances[containerId];
        }
        var container = document.getElementById(containerId);
        if (!container) return;
        if (!container.style.height) container.style.height = '260px';

        var map = L.map(containerId, {
            center: [30, 10],
            zoom: 2,
            zoomControl: true,
            attributionControl: true,
            scrollWheelZoom: false,
            worldCopyJump: false
        });

        L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
            attribution: '© OpenStreetMap © CARTO',
            subdomains: 'abcd',
            maxZoom: 19,
            crossOrigin: true
        }).addTo(map);

        var maxCount = points.reduce(function (m, p) { return Math.max(m, p.count); }, 1);

        points.forEach(function (p) {
            var coords = resolve(p.name);
            if (!coords) return;
            var ratio = p.count / maxCount;
            var radius = Math.round(6 + ratio * 26);
            var circle = L.circleMarker(coords, {
                radius: radius,
                fillColor: '#1B8FFF',
                fillOpacity: 0.65,
                color: '#78c8ff',
                weight: 1.5,
                opacity: 0.9
            }).addTo(map);
            circle.bindTooltip(
                '<strong>' + p.name + '</strong><br>' + p.count.toLocaleString() + ' users',
                { direction: 'top', offset: [0, -radius], className: 'leaflet-dc-tip' }
            );
        });

        setTimeout(function () { map.invalidateSize(); }, 100);
        _mapInstances[containerId] = map;
    }

    return {
        init: function (containerId, points) {
            if (typeof L === 'undefined') {
                var tries = 0;
                var iv = setInterval(function () {
                    tries++;
                    if (typeof L !== 'undefined') {
                        clearInterval(iv);
                        doInit(containerId, points);
                    } else if (tries > 20) {
                        clearInterval(iv);
                        console.warn('Leaflet not loaded after 2s');
                    }
                }, 100);
            } else {
                doInit(containerId, points);
            }
        }
    };
})();