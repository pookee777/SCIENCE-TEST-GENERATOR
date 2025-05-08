/**
 * Charts functionality for Physics Test Generator
 */

/**
 * Creates a performance line chart showing score trends over time
 * @param {string} canvasId - ID of the canvas element
 * @param {Array} labels - Array of labels (dates/test names)
 * @param {Array} data - Array of score values
 * @param {string} label - Label for the dataset
 */
function createPerformanceLineChart(canvasId, labels, data, label = 'Score (%)') {
    const ctx = document.getElementById(canvasId).getContext('2d');
    
    // Create gradient fill
    const gradient = ctx.createLinearGradient(0, 0, 0, 400);
    gradient.addColorStop(0, 'rgba(54, 162, 235, 0.8)');
    gradient.addColorStop(1, 'rgba(54, 162, 235, 0.1)');
    
    const chart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: label,
                data: data,
                backgroundColor: gradient,
                borderColor: 'rgba(54, 162, 235, 1)',
                borderWidth: 2,
                tension: 0.3,
                fill: true,
                pointBackgroundColor: 'rgba(54, 162, 235, 1)',
                pointBorderColor: '#fff',
                pointBorderWidth: 2,
                pointRadius: 5,
                pointHoverRadius: 7
            }]
        },
        options: {
            responsive: true,
            scales: {
                y: {
                    beginAtZero: true,
                    max: 100,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    }
                },
                x: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    }
                }
            },
            plugins: {
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    callbacks: {
                        label: function(context) {
                            return `Score: ${context.parsed.y}%`;
                        }
                    }
                },
                legend: {
                    labels: {
                        font: {
                            size: 14
                        }
                    }
                }
            },
            interaction: {
                mode: 'nearest',
                intersect: false
            },
            animations: {
                tension: {
                    duration: 1000,
                    easing: 'linear'
                }
            }
        }
    });
    
    return chart;
}

/**
 * Creates a radar chart showing performance across different chapters
 * @param {string} canvasId - ID of the canvas element
 * @param {Array} labels - Array of chapter names
 * @param {Array} data - Array of score values
 */
function createChapterRadarChart(canvasId, labels, data) {
    const ctx = document.getElementById(canvasId).getContext('2d');
    
    // Determine colors based on performance
    const backgroundColors = [];
    const borderColors = [];
    
    data.forEach(value => {
        if (value >= 80) {
            backgroundColors.push('rgba(40, 167, 69, 0.2)');
            borderColors.push('rgba(40, 167, 69, 1)');
        } else if (value >= 60) {
            backgroundColors.push('rgba(23, 162, 184, 0.2)');
            borderColors.push('rgba(23, 162, 184, 1)');
        } else if (value >= 40) {
            backgroundColors.push('rgba(255, 193, 7, 0.2)');
            borderColors.push('rgba(255, 193, 7, 1)');
        } else {
            backgroundColors.push('rgba(220, 53, 69, 0.2)');
            borderColors.push('rgba(220, 53, 69, 1)');
        }
    });
    
    const chart = new Chart(ctx, {
        type: 'radar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Performance (%)',
                data: data,
                backgroundColor: 'rgba(54, 162, 235, 0.2)',
                borderColor: 'rgba(54, 162, 235, 1)',
                borderWidth: 2,
                pointBackgroundColor: borderColors,
                pointBorderColor: '#fff',
                pointHoverBackgroundColor: '#fff',
                pointHoverBorderColor: borderColors,
                pointRadius: 5
            }]
        },
        options: {
            scales: {
                r: {
                    beginAtZero: true,
                    max: 100,
                    ticks: {
                        stepSize: 20,
                        backdropColor: 'transparent'
                    },
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    },
                    angleLines: {
                        color: 'rgba(255, 255, 255, 0.2)'
                    },
                    pointLabels: {
                        font: {
                            size: 12
                        }
                    }
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return `${context.label}: ${context.parsed.r}%`;
                        }
                    }
                }
            }
        }
    });
    
    return chart;
}

/**
 * Creates a bar chart for comparing student performances
 * @param {string} canvasId - ID of the canvas element
 * @param {Array} labels - Array of student names
 * @param {Array} data - Array of score values
 */
function createStudentComparisonChart(canvasId, labels, data) {
    const ctx = document.getElementById(canvasId).getContext('2d');
    
    // Generate colors based on scores
    const backgroundColors = data.map(score => {
        if (score >= 80) {
            return 'rgba(40, 167, 69, 0.7)';
        } else if (score >= 60) {
            return 'rgba(23, 162, 184, 0.7)';
        } else if (score >= 40) {
            return 'rgba(255, 193, 7, 0.7)';
        } else {
            return 'rgba(220, 53, 69, 0.7)';
        }
    });
    
    const borderColors = data.map(score => {
        if (score >= 80) {
            return 'rgba(40, 167, 69, 1)';
        } else if (score >= 60) {
            return 'rgba(23, 162, 184, 1)';
        } else if (score >= 40) {
            return 'rgba(255, 193, 7, 1)';
        } else {
            return 'rgba(220, 53, 69, 1)';
        }
    });
    
    const chart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Score (%)',
                data: data,
                backgroundColor: backgroundColors,
                borderColor: borderColors,
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            scales: {
                y: {
                    beginAtZero: true,
                    max: 100,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    }
                },
                x: {
                    grid: {
                        display: false
                    }
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return `Score: ${context.parsed.y}%`;
                        }
                    }
                }
            },
            animation: {
                duration: 1500
            }
        }
    });
    
    return chart;
}

/**
 * Creates a doughnut chart showing question type distribution
 * @param {string} canvasId - ID of the canvas element
 * @param {Array} labels - Array of question types
 * @param {Array} data - Array of counts for each type
 */
function createQuestionTypeChart(canvasId, labels, data) {
    const ctx = document.getElementById(canvasId).getContext('2d');
    
    const backgroundColors = [
        'rgba(54, 162, 235, 0.7)',  // Multiple Choice
        'rgba(255, 206, 86, 0.7)',  // True/False
        'rgba(75, 192, 192, 0.7)',  // Numerical
        'rgba(153, 102, 255, 0.7)'  // Descriptive
    ];
    
    const borderColors = [
        'rgba(54, 162, 235, 1)',
        'rgba(255, 206, 86, 1)',
        'rgba(75, 192, 192, 1)',
        'rgba(153, 102, 255, 1)'
    ];
    
    const chart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: data,
                backgroundColor: backgroundColors,
                borderColor: borderColors,
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: {
                    position: 'bottom'
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const label = context.label || '';
                            const value = context.parsed || 0;
                            const total = context.dataset.data.reduce((acc, val) => acc + val, 0);
                            const percentage = Math.round((value / total) * 100);
                            return `${label}: ${value} (${percentage}%)`;
                        }
                    }
                }
            },
            animation: {
                animateRotate: true,
                animateScale: true
            }
        }
    });
    
    return chart;
}
