(function () {
    'use strict';

    AOS.init({
        duration: 800,
        easing: 'slide',
        once: true
    });

    var preloader = function () {
        var loader = document.querySelector('.loader');
        var overlay = document.getElementById('overlayer');

        function fadeOut(el) {
            el.style.opacity = 1;
            (function fade() {
                if ((el.style.opacity -= .1) < 0) {
                    el.style.display = "none";
                } else {
                    requestAnimationFrame(fade);
                }
            })();
        }

        setTimeout(function () {
            fadeOut(loader);
            fadeOut(overlay);
        }, 200);
    };
    preloader();

    function parseUrlParams() {
        const urlParams = new URLSearchParams(window.location.search);
        return {
            exception_mode: urlParams.get('exception_mode'),
            car_name: urlParams.get('car_name'),
            issue: urlParams.get('issue')
        };
    }

    function loadHotIssueSidebar() {
        const introSection = document.getElementById('intro-section');
        if (introSection) {
            introSection.style.display = 'none';
        }

        fetch('/api/exception_mode=hot_issue')
            .then(response => {
                if (!response.ok) {
                    throw new Error('Server error: ' + response.status);
                }
                return response.json();
            })
            .then(data => {
                const hotIssueList = document.getElementById('hotIssueList');
                hotIssueList.innerHTML = '';

                if (data.length === 0) {
                    hotIssueList.innerHTML = '';
                } else {
                    data.forEach(issue => {
                        const dateMatch = issue.match(/\(\d{4}-\d{2}-\d{2}\)$/);
                        const date = dateMatch ? dateMatch[0] : '';
                        const cleanIssue = issue.replace(/\s*\(\d{4}-\d{2}-\d{2}\)$/, '');
                        const [carName, issueName] = cleanIssue.split(' - ');
                        const issueItem = document.createElement('a');
                        issueItem.href = `index.html?exception_mode=hot_issue&car_name=${encodeURIComponent(carName)}&issue=${encodeURIComponent(issueName)}`;
                        issueItem.textContent = `${carName} - ${issueName}`;
                        issueItem.innerHTML += `<br>${date}`;
                        hotIssueList.appendChild(issueItem);
                    });

                    const hotIssueContainer = document.querySelector('.hot-issue-list-container');
                    hotIssueContainer.style.display = 'block';
                    hotIssueContainer.style.flex = '1';
                }
            })
            .catch(error => {
                console.error('Error fetching hot issues:', error);
                const hotIssueList = document.getElementById('hotIssueList');
                hotIssueList.innerHTML = '';
            });
    }

    function loadIssueListForCar(car_name) {
        const introSection = document.getElementById('intro-section');
        if (introSection) {
            introSection.style.display = 'none';
        }

        const urlParams = new URLSearchParams(window.location.search);
        const exceptionMode = urlParams.get('exception_mode');
        if (exceptionMode === 'hot_issue') {
            const defectListContainer = document.querySelector('.defect-list-container');
            if (defectListContainer) {
                defectListContainer.style.display = 'none';
            }
            return;
        }

        fetch('/api/config')
            .then(response => response.json())
            .then(config => {
                const defectList = document.getElementById('defectList');
                const defectTypes = config.defectTypes;

                defectList.innerHTML = '';

                defectTypes.forEach(defect => {
                    const defectLink = document.createElement('a');
                    defectLink.href = `index.html?car_name=${encodeURIComponent(car_name)}&issue=${encodeURIComponent(defect)}`;
                    defectLink.textContent = defect;
                    defectList.appendChild(defectLink);
                });
            })
            .catch(error => {
                console.error('Error fetching config data:', error);
            });
    }

    function plotDataForCarAndIssue(car_name, issue) {
        const chartSection = document.getElementById('chart-section');
        const summarySection = document.getElementById('summary-section');
        const defectListContainer = document.querySelector('.defect-list-container');
        chartSection.style.display = 'block';
        summarySection.style.display = 'block';
        defectListContainer.style.display = 'none';

        fetch(`/api/data?car_name=${encodeURIComponent(car_name)}&issue=${encodeURIComponent(issue)}`)
            .then(response => {
                if (!response.ok) {
                    throw new Error('Server error: ' + response.status);
                }
                return response.json();
            })
            .then(data => {
                if (data.error) {
                    console.error(data.error);
                    chartSection.style.display = 'none';
                    summarySection.style.display = 'none';
                    return;
                }
                renderChart(data);
                displaySummarySection(data);
            })
            .catch(error => {
                console.error('Error fetching data:', error);
                chartSection.style.display = 'none';
                summarySection.style.display = 'none';
            });
    }

    function displaySummarySection(data) {
        const summarySection = document.getElementById('summary-section');
        if (data.today_data) {
            const today = new Date();
            const formattedDate = today.toISOString().split('T')[0];
            const linkHtml = data.url 
                ? `<a href="${data.url}" target="_blank" class="link-button">
                    <img src="/images/check.png" alt="Check Icon">
                    결함 게시물 바로가기
                </a>`
                : `<span class="link-button-disabled">
                    <img src="/images/check.png" alt="Check Icon">
                    결함 게시물 바로가기
                </span>`;

            const summaryText = `
                <div class="summary-header" style="display: flex; justify-content: space-between; align-items: center;">
                    <p class="summary-date-car-issue" style="margin-right: auto;">
                        ${formattedDate}&nbsp;&nbsp;&nbsp;${data.today_data.car_name} - ${data.today_data.single_issue}
                    </p>
                    ${linkHtml}
                </div>
                    <div class="row stat-cards">
                        <div class="col-md-6 col-xl-3">
                            <article class="stat-cards-item">
                                <div class="stat-cards-icon post">
                                    <img src="images/Post.png" alt="Post Icon">
                                </div>
                                <div class="stat-cards-info">
                                    <p class="stat-cards-info__num">${data.today_data.num_posts}</p>
                                    <p class="stat-cards-info__title">결함 게시물 수</p>
                                </div>
                            </article>
                        </div>
                        <div class="col-md-6 col-xl-3">
                            <article class="stat-cards-item">
                                <div class="stat-cards-icon like">
                                    <img src="images/Like.png" alt="Like Icon">
                                </div>
                                <div class="stat-cards-info">
                                    <p class="stat-cards-info__num">${data.today_data.total_likes}</p>
                                    <p class="stat-cards-info__title">결함 좋아요 수</p>
                                </div>
                            </article>
                        </div>
                        <div class="col-md-6 col-xl-3">
                            <article class="stat-cards-item">
                                <div class="stat-cards-icon view">
                                    <img src="images/View.png" alt="View Icon">
                                </div>
                                <div class="stat-cards-info">
                                    <p class="stat-cards-info__num">${data.today_data.total_views}</p>
                                    <p class="stat-cards-info__title">결함 조회수</p>
                                </div>
                            </article>
                        </div>
                        <div class="col-md-6 col-xl-3">
                            <article class="stat-cards-item">
                                <div class="stat-cards-icon comment">
                                    <img src="images/Comment.png" alt="Comment Icon">
                                </div>
                                <div class="stat-cards-info">
                                    <p class="stat-cards-info__num">${data.today_data.issue_mentions}</p>
                                    <p class="stat-cards-info__title">결함 언급 수</p>
                                </div>
                            </article>
                        </div>
                    </div>
            `;
            summarySection.innerHTML = summaryText;
        } else {
            const noDataText = `<span class="summary-no-data">
                                    금일 결함 이슈가 없습니다. 휴~&nbsp;&nbsp;
                                    <img src="images/no_issue.jpg" alt="No Issue" class="no-issue-image">
                                </span>`;
            summarySection.innerHTML = noDataText;
            summarySection.style.display = 'block';
        }
    }

    function renderChart(data) {
        const canvas = document.getElementById('issueChart');
        if (!canvas) {
            console.error("Cannot find the canvas element with id 'issueChart'.");
            return;
        }
        const ctx = canvas.getContext('2d');
        if (!ctx) {
            console.error("Cannot get the context for the canvas.");
            return;
        }

        const summarySection = document.getElementById('summary-section');
        if (!summarySection) {
            console.error("Cannot find the summary-section element.");
            return;
        }

        const annotationLines = data.vertical_lines.map(line => ({
            id: `annotation-${line.label}`,
            type: 'line',
            scaleID: 'x',
            value: line.date,
            borderColor: line.color,
            borderWidth: 2,
            label: {
                content: line.label,
                enabled: true,
                position: 'start',
                backgroundColor: line.color,
                color: '#fff'
            }
        }));

        if (window.issueChart && window.issueChart.data && window.issueChart.options) {
            window.issueChart.data.labels = data.labels || [];
            window.issueChart.data.datasets = data.datasets || [];
            if (window.issueChart.options.plugins && window.issueChart.options.plugins.annotation) {
                window.issueChart.options.plugins.annotation.annotations = annotationLines;
            } else {
                console.error("window.issueChart.options.plugins.annotation is undefined");
            }
            window.issueChart.update();
        } else {
            window.issueChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: data.labels || [],
                    datasets: data.datasets || []
                },
                options: {
                    responsive: true,
                    scales: {
                        y: {
                            beginAtZero: true
                        }
                    },
                    elements: {
                        line: {
                            tension: 0.4
                        }
                    },
                    spanGaps: true,
                    plugins: {
                        annotation: {
                            annotations: annotationLines
                        },
                        legend: {
                            onClick: function (e, legendItem) {
                                const chart = this.chart;
                                const index = legendItem.datasetIndex;
                                if (index === undefined || index === null) {
                                    console.error('No dataset index found in legendItem', legendItem);
                                    return;
                                }
                                if (!chart) {
                                    console.error('Chart instance not found');
                                    return;
                                }
                                const meta = chart.getDatasetMeta(index);
                                if (!meta) {
                                    console.error('No dataset meta found for index:', index);
                                    return;
                                }
                                meta.hidden = meta.hidden === null ? !chart.data.datasets[index].hidden : null;
                                const datasetLabel = chart.data.datasets[index].label;
                                const parts = datasetLabel.split(" (")[0];
                                const annotationId = `annotation-news: ${parts}`;
                                const matchingAnnotations = chart.options.plugins.annotation.annotations.filter(ann => ann.id === annotationId);
                                matchingAnnotations.forEach(annotation => {
                                    annotation.display = !meta.hidden;
                                });
                                chart.update();
                            }
                        }
                    },
                }
            });
        }
    }

    function handlePageLoad() {
        const params = parseUrlParams();
        const carName = params.car_name;
        const chartSection = document.getElementById('chart-section');
        const summarySection = document.getElementById('summary-section');
        const defectListContainer = document.querySelector('.defect-list-container');
        const introSection = document.getElementById('intro-section');
        fetch('/api/config')
            .then(response => response.json())
            .then(config => {
                const vehicles = config.vehicles;

                if (carName && vehicles[carName]) {
                    const vehicle = vehicles[carName];
                    document.querySelector('.hero-2').style.backgroundImage = `url(${vehicle.image})`;
                    document.querySelector('title').innerText = `${vehicle.name} | HyunDaTa`;
                    introSection.style.display = 'none';
                }

                if (params.exception_mode === 'hot_issue' && !params.car_name && !params.issue) {
                    loadHotIssueSidebar();
                    chartSection.style.display = 'none';
                    summarySection.style.display = 'none';
                    defectListContainer.style.display = 'none';
                    introSection.style.display = 'none';
                    return;
                }

                if (params.exception_mode === 'hot_issue' && params.car_name && params.issue) {
                    loadHotIssueSidebar();
                    plotDataForCarAndIssue(params.car_name, params.issue);
                    defectListContainer.style.display = 'none';
                    introSection.style.display = 'none';
                    return;
                }

                if (params.car_name && !params.issue) {
                    loadIssueListForCar(params.car_name);
                    chartSection.style.display = 'none';
                    summarySection.style.display = 'none';
                    introSection.style.display = 'none';
                    return;
                }

                if (params.car_name && params.issue && !params.exception_mode) {
                    plotRecommendationData(params.car_name, params.issue);
                    summarySection.style.display = 'block';
                    introSection.style.display = 'none';
                    return;
                }

                chartSection.style.display = 'none';
                summarySection.style.display = 'none';
            })
            .catch(error => {
                console.error('Error fetching config data:', error);
            });
    }

    document.addEventListener('DOMContentLoaded', () => {
        handlePageLoad();
        document.getElementById('hotIssueList').addEventListener('click', function (event) {
            if (event.target.tagName === 'A') {
                event.preventDefault();
                const url = new URL(event.target.href);
                window.history.pushState({}, '', url);
                handlePageLoad();
            }
        });
        window.addEventListener('popstate', handlePageLoad);
        fetch('/api/today_data')
            .then(response => response.json())
            .then(data => {
                if (!data.error) {
                    const dashboard = document.getElementById('dashboard-data');
                    data.forEach(item => {
                        const row = document.createElement('tr');
                        row.innerHTML = `
                            <td>${item.car_name}</td>
                            <td>${item.issue}</td>
                            <td>${item.num_posts}</td>
                            <td>${item.total_likes}</td>
                            <td>${item.total_views}</td>
                            <td>${item.issue_mentions}</td>
                        `;
                        dashboard.appendChild(row);
                    });
                }
            })
            .catch(error => {
                console.error('Error fetching today\'s data:', error);
            });
    });
})();
