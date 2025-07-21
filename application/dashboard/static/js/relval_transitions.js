
// ---------- RelVal transition fetching and parsing ----------

const timeFilterSelector = document.getElementById('time-filter');
const timeUnitSelector = document.getElementById('time-unit');
const batchNameSelector = document.getElementById('batch-name-filter');
const chartsContainer = document.getElementById('charts-container');
let allTransitions = [];

function getShortenedLabel(unitName) {
  if (unitName == "minutes") {
    return "min"
  } else if (unitName == "hours") {
    return "h"
  } else {
    return "d"
  }
}

function displayUpdateTime(timestamp) {
    const displayElement = document.getElementById('last-updated-time');
    if (!displayElement) return;

    const updateDate = new Date(timestamp * 1000);

    const normalDate = updateDate.toLocaleString(undefined, {
        year: 'numeric', month: 'long', day: 'numeric',
        hour: 'numeric', minute: '2-digit'
    });

    const relativeDate = new Intl.RelativeTimeFormat('en', { numeric: 'auto' });
    const secondsAgo = Math.floor((updateDate.getTime() - Date.now()) / 1000);

    let relativeTime;
    if (Math.abs(secondsAgo) < 60) {
        relativeTime = relativeDate.format(secondsAgo, 'second');
    } else if (Math.abs(secondsAgo) < 3600) {
        relativeTime = relativeDate.format(Math.floor(secondsAgo / 60), 'minute');
    } else {
        relativeTime = relativeDate.format(Math.floor(secondsAgo / 3600), 'hour');
    }
    displayElement.textContent = `${normalDate} (${relativeTime})`;
}

function populateBatchNameSelector() {
    const batchNames = [...new Set(allTransitions.map(t => t.batch_name))].sort();
    batchNames.forEach(name => {
        if (name) {
            batchNameSelector.add(new Option(name, name));
        }
    });
}

function renderChart(container, transitionType, transitions, unit) {
  const chartBlock = document.createElement('div');
  chartBlock.className = 'chart-block';

  const title = document.createElement('h4');
  title.textContent = transitionType;

  const canvasContainer = document.createElement('div');
  canvasContainer.className = 'chart-container';
  const canvas = document.createElement('canvas');
  canvasContainer.appendChild(canvas);

  chartBlock.appendChild(title);
  chartBlock.appendChild(canvasContainer);
  container.appendChild(chartBlock);

  let divisor = 60;
  let step = 15;
  if (unit === 'hours') {
    divisor = 3600;
    step = 1;
  } else if (unit === 'days') {
    divisor = 86400;
    step = 1;
  }

  // Group durations into the predefined bins
  const bins = transitions.reduce((acc, t) => {
    const durationInUnit = t.duration_seconds / divisor;
    const binStart = Math.floor(durationInUnit / step) * step;
    acc[binStart] = (acc[binStart] || 0) + 1;
    return acc;
  }, {});

  // Prepare the sorted data and labels for the chart
  const sortedBinStarts = Object.keys(bins).map(Number).sort((a, b) => a - b);
  const chartData = sortedBinStarts.map(start => bins[start]);
  // Create labels that show a range, e.g., "15-30 minutes"
  const chartLabels = sortedBinStarts.map(start => `${start} - ${start + step} ${getShortenedLabel(unit)}`);

  new Chart(canvas.getContext('2d'), {
    type: 'bar',
    data: {
      labels: chartLabels,
      datasets: [{
        label: 'Number of transitions',
        data: chartData,
        backgroundColor: 'rgb(5, 155, 255)'
      }]
    },
    options: {
      scales: {
        y: { beginAtZero: true, title: { display: true, text: 'Count' }, suggestedMax: 3, ticks: { precision: 0 } },
        x: { title: { display: true, text: `Time in ${unit}` } }
      }
    }
  });
}

function renderAllCharts() {
  chartsContainer.innerHTML = '';
  const unit = timeUnitSelector.value;
  const timeFilterDays = timeFilterSelector.value;
  const selectedBatch = batchNameSelector.value;

  let filteredTransitions = allTransitions;

  if (timeFilterDays !== 'all') {
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - parseInt(timeFilterDays));
    const cutoffTimestamp = Math.floor(cutoffDate.getTime() / 1000);
    filteredTransitions = allTransitions.filter(t => t.start >= cutoffTimestamp);
  }
  
  if (selectedBatch !== 'all') {
    filteredTransitions = filteredTransitions.filter(t => t.batch_name === selectedBatch);
  }

  const groupedByType = filteredTransitions.reduce((acc, t) => {
    const type = `${t.from} -> ${t.to}`;
    if (!acc[type]) acc[type] = [];
    acc[type].push(t);
    return acc;
  }, {});

  for (const transitionType in groupedByType) {
    const transitions = groupedByType[transitionType];
    renderChart(chartsContainer, transitionType, transitions, unit);
  }
}

async function initializeDashboard() {
  const dataUrl = document.getElementById('charts-container').dataset.url;
  try {
    const response = await fetch(dataUrl);
    if (!response.ok) throw new Error((await response.json()).error || 'Failed to fetch data');
    const { last_updated, results } = await response.json();
    allTransitions = results;
    populateBatchNameSelector();
    renderAllCharts();
    displayUpdateTime(last_updated);
  } catch (error) {
    console.error('Error initializing dashboard:', error);
    chartsContainer.innerHTML = `<p style="color: red;">Error: ${error.message}</p>`;
  }
}

[timeFilterSelector, timeUnitSelector, batchNameSelector].forEach(selector => {
    selector.addEventListener('change', renderAllCharts);
});

initializeDashboard();