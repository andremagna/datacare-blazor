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
    // FIX 4: base64 download — works in all modern browsers
    downloadBase64: function (filename, mime, b64) {
        var a = document.createElement('a');
        a.href = 'data:' + mime + ';base64,' + b64;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        setTimeout(function () { document.body.removeChild(a); }, 200);
    },
    // PDF via browser print dialog
    downloadPdf: function () {
        var style = document.createElement('style');
        style.id = '__pdf_print';
        style.textContent = [
            '@media print {',
            '  body > * { display:none !important; }',
            '  .dashboard-page { display:block !important; }',
            '  .dashboard-page * { display:revert !important; }',
            '  .dash-controls { display:none !important; }',
            '}'
        ].join('');
        document.head.appendChild(style);
        window.onafterprint = function () {
            var s = document.getElementById('__pdf_print');
            if (s) s.parentNode.removeChild(s);
        };
        window.print();
    }
};

// ── World bubble map — pure SVG, no external dependencies ────────────────
// Country centroids (lon, lat). Equirectangular projection onto viewBox.
window.datacareMap = (function () {

    var CENTROIDS = {
        'Italy': [12.57, 41.87], 'Germany': [10.45, 51.17], 'France': [2.21, 46.23],
        'Spain': [3.75, 40.46], 'United Kingdom': [-3.44, 55.38], 'UK': [-3.44, 55.38],
        'Netherlands': [5.29, 52.13], 'Belgium': [4.47, 50.50], 'Switzerland': [8.23, 46.82],
        'Austria': [14.55, 47.52], 'Portugal': [-8.22, 39.40], 'Poland': [19.14, 51.92],
        'Czech Republic': [15.47, 49.82], 'Czechia': [15.47, 49.82],
        'Hungary': [19.50, 47.16], 'Romania': [24.97, 45.94], 'Sweden': [18.64, 60.13],
        'Norway': [8.47, 60.47], 'Denmark': [9.50, 56.26], 'Finland': [25.73, 61.92],
        'Greece': [21.82, 39.07], 'Croatia': [15.20, 45.10], 'Serbia': [21.01, 44.02],
        'Slovakia': [19.70, 48.67], 'Bulgaria': [25.49, 42.73], 'Luxembourg': [6.13, 49.82],
        'Ireland': [-8.24, 53.41], 'Turkey': [35.24, 38.96], 'Russia': [105.32, 61.52],
        'Ukraine': [31.17, 48.38], 'United States': [-101.3, 37.09], 'USA': [-101.3, 37.09],
        'Canada': [-96.80, 56.13], 'Mexico': [-102.55, 23.63], 'Brazil': [-51.93, -14.24],
        'Argentina': [-63.62, -38.42], 'Chile': [-71.54, -35.68], 'Colombia': [-74.30, 4.57],
        'Peru': [-75.02, -9.19], 'China': [104.19, 35.86], 'Japan': [138.25, 36.20],
        'South Korea': [127.77, 35.91], 'India': [78.96, 20.59], 'Indonesia': [113.92, -0.79],
        'Australia': [133.78, -25.27], 'New Zealand': [174.89, -40.90],
        'South Africa': [25.08, -28.48], 'Egypt': [30.80, 26.82], 'Nigeria': [8.68, 9.08],
        'Kenya': [37.91, -0.02], 'Morocco': [-7.09, 31.79],
        'United Arab Emirates': [53.85, 23.42], 'UAE': [53.85, 23.42],
        'Saudi Arabia': [45.08, 23.88], 'Israel': [34.85, 31.05],
        'Qatar': [51.18, 25.35], 'Singapore': [103.82, 1.35], 'Hong Kong': [114.17, 22.32],
        'Taiwan': [120.96, 23.70], 'Thailand': [100.99, 15.87], 'Vietnam': [108.28, 14.06],
        'Malaysia': [109.70, 4.21], 'Philippines': [122.88, 12.88],
        'Pakistan': [69.34, 30.38], 'Bangladesh': [90.36, 23.68]
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

    // Equirectangular: lon [-180,180] → x [0,W], lat [90,-90] → y [0,H]
    function proj(lon, lat, W, H) {
        return [(lon + 180) / 360 * W, (90 - lat) / 180 * H];
    }

    var NS = 'http://www.w3.org/2000/svg';
    function el(tag, attrs) {
        var e = document.createElementNS(NS, tag);
        for (var k in attrs) e.setAttribute(k, attrs[k]);
        return e;
    }

    return {
        init: function (containerId, points) {
            var container = document.getElementById(containerId);
            if (!container) return;
            container.innerHTML = '';

            var W = 860, H = 430;
            var svg = el('svg', {
                viewBox: '0 0 ' + W + ' ' + H,
                preserveAspectRatio: 'xMidYMid meet',
                style: 'width:100%;height:100%;display:block;background:#0a0f1e;'
            });

            // ── Graticule ──────────────────────────────────────────────
            for (var lon = -180; lon <= 180; lon += 30) {
                var x = (lon + 180) / 360 * W;
                svg.appendChild(el('line', { x1: x, y1: 0, x2: x, y2: H, stroke: '#111d30', 'stroke-width': '0.5' }));
            }
            for (var lat = -90; lat <= 90; lat += 30) {
                var y = (90 - lat) / 180 * H;
                svg.appendChild(el('line', { x1: 0, y1: y, x2: W, y2: y, stroke: '#111d30', 'stroke-width': '0.5' }));
            }

            // ── Land polygons (simplified Natural Earth, equirectangular) ──
            var lands = [
                // North America mainland
                'M176,72 L186,68 L212,70 L230,66 L256,72 L270,68 L278,78 L280,90 L272,102 L260,112 ' +
                'L258,122 L268,128 L274,140 L268,152 L262,160 L252,168 L242,178 L232,186 L220,188 ' +
                'L210,182 L200,172 L192,162 L186,152 L182,138 L178,124 L172,110 L170,96 L174,82 Z',
                // Greenland
                'M326,22 L370,18 L390,28 L388,44 L374,54 L354,58 L336,52 L322,40 Z',
                // South America
                'M242,188 L252,184 L268,188 L280,196 L286,210 L284,224 L278,238 ' +
                'L272,252 L264,268 L258,280 L252,296 L248,310 L244,322 L242,330 ' +
                'L238,322 L236,308 L234,294 L232,278 L234,262 L238,248 L240,232 ' +
                'L238,216 L236,202 Z',
                // Europe
                'M430,56 L448,54 L466,58 L476,68 L480,78 L472,86 L462,90 L456,98 ' +
                'L448,102 L440,96 L432,90 L428,80 L426,68 Z',
                // Scandinavia
                'M448,38 L460,34 L472,40 L478,52 L470,58 L460,56 L450,50 Z',
                // UK
                'M426,56 L432,50 L436,56 L432,62 L428,60 Z',
                // Africa
                'M462,110 L484,106 L502,108 L516,118 L524,132 L528,150 L524,168 ' +
                'L518,186 L510,202 L502,220 L494,238 L484,248 L472,252 L460,244 ' +
                'L450,232 L444,216 L442,200 L446,184 L450,168 L454,152 L456,136 ' +
                'L458,120 Z',
                // Middle East
                'M516,118 L534,112 L550,114 L560,122 L558,134 L548,140 L536,138 L522,130 Z',
                // Russia / Central Asia (simplified)
                'M480,40 L520,36 L570,38 L620,42 L660,48 L690,52 L700,62 L690,70 ' +
                'L670,72 L640,68 L610,64 L580,66 L556,70 L532,72 L510,70 L490,66 ' +
                'L476,60 Z',
                // South Asia
                'M580,120 L600,114 L618,118 L626,132 L622,148 L610,158 L596,156 ' +
                'L584,148 L578,134 Z',
                // East Asia
                'M640,68 L670,72 L686,82 L688,98 L680,112 L668,118 L654,114 ' +
                'L644,104 L638,90 L636,78 Z',
                // Southeast Asia
                'M670,140 L684,136 L694,142 L692,152 L682,156 L672,150 Z',
                // Japan
                'M710,78 L716,72 L720,78 L716,84 Z',
                'M714,68 L720,64 L724,70 L718,74 Z',
                // Australia
                'M690,250 L720,242 L744,244 L762,254 L768,270 L764,288 L752,300 ' +
                'L736,306 L716,304 L700,296 L690,280 L688,264 Z',
                // New Zealand
                'M780,298 L786,292 L790,298 L786,306 Z'
            ];

            lands.forEach(function (d) {
                svg.appendChild(el('path', {
                    d: d, fill: '#1e2f4a', stroke: '#0d1a2e', 'stroke-width': '0.8'
                }));
            });

            // ── Bubbles ────────────────────────────────────────────────
            var tip = document.getElementById('chart-tooltip');
            var maxCount = points.reduce(function (m, p) { return Math.max(m, p.count); }, 1);

            points.forEach(function (p) {
                var coords = resolve(p.name);
                if (!coords) return;
                var xy = proj(coords[0], coords[1], W, H);
                var r = 4 + (p.count / maxCount) * 28;

                var circle = el('circle', {
                    cx: xy[0].toFixed(1), cy: xy[1].toFixed(1), r: r.toFixed(1),
                    fill: '#1B8FFF', 'fill-opacity': '0.72',
                    stroke: '#ffffff', 'stroke-width': '1.0'
                });
                circle.style.cursor = 'pointer';

                circle.addEventListener('mouseenter', function (e) {
                    circle.setAttribute('fill-opacity', '0.92');
                    if (tip) {
                        tip.textContent = p.name + ': ' + p.count.toLocaleString() + ' users';
                        tip.style.display = 'block';
                        _pos(tip, e);
                    }
                });
                circle.addEventListener('mousemove', function (e) { if (tip) _pos(tip, e); });
                circle.addEventListener('mouseleave', function () {
                    circle.setAttribute('fill-opacity', '0.72');
                    if (tip) tip.style.display = 'none';
                });

                svg.appendChild(circle);
            });

            container.appendChild(svg);
        }
    };

    function _pos(el, e) {
        var x = e.clientX + 14, y = e.clientY - 38;
        var w = el.offsetWidth || 160;
        if (x + w > window.innerWidth - 8) x = e.clientX - w - 14;
        el.style.left = x + 'px'; el.style.top = y + 'px';
    }
})();
