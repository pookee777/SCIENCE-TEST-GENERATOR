/**
 * Test timer functionality for Physics Test Generator
 */

/**
 * Initialize a countdown timer
 * @param {number} seconds - Duration in seconds
 * @param {string} displaySelector - CSS selector for the element to display the timer
 * @param {function} onTimeUp - Callback function to execute when timer completes
 */
function initTimer(seconds, displaySelector, onTimeUp) {
    const display = document.querySelector(displaySelector);
    let timeLeft = parseInt(seconds);

    function updateDisplay() {
        const hours = Math.floor(timeLeft / 3600);
        const minutes = Math.floor((timeLeft % 3600) / 60);
        const seconds = timeLeft % 60;
        
        display.textContent = hours.toString().padStart(2, '0') + ':' +
                            minutes.toString().padStart(2, '0') + ':' +
                            seconds.toString().padStart(2, '0');
    }

    function tick() {
        if (timeLeft <= 0) {
            clearInterval(timer);
            onTimeUp();
            return;
        }
        updateDisplay();
        timeLeft--;
    }

    // Initial display
    updateDisplay();
    // Update every second
    const timer = setInterval(tick, 1000);
}

/**
 * Update the timer display with formatted time
 * @param {number} seconds - Time in seconds
 * @param {Element} display - DOM element to update
 */
function updateTimerDisplay(seconds, display) {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    
    const formattedTime = [
        hours.toString().padStart(2, '0'),
        minutes.toString().padStart(2, '0'),
        secs.toString().padStart(2, '0')
    ].join(':');
    
    display.textContent = formattedTime;
}

/**
 * Update the timer progress bar if it exists
 * @param {number} currentTime - Current time in seconds
 * @param {number} totalTime - Total duration in seconds
 */
function updateTimerProgress(currentTime, totalTime) {
    const progressBar = document.getElementById('timer-progress');
    if (progressBar) {
        const percentage = (currentTime / totalTime) * 100;
        progressBar.style.width = percentage + '%';
        
        // Change color based on time remaining
        if (percentage <= 25) {
            progressBar.className = 'progress-bar bg-danger';
        } else if (percentage <= 50) {
            progressBar.className = 'progress-bar bg-warning';
        } else {
            progressBar.className = 'progress-bar bg-success';
        }
    }
}

/**
 * Flash the timer display when time is running low
 * @param {Element} display - DOM element containing the timer
 * @param {number} timer - Current time in seconds
 */
function flashTimer(display, timer) {
    // Add flashing effect when time is running low
    if (timer <= 60) {
        // Last minute - flash red rapidly
        display.classList.add('text-danger');
        display.classList.add('fw-bold');
        if (timer % 2 === 0) {
            display.style.opacity = '0.5';
        } else {
            display.style.opacity = '1';
        }
    } else if (timer <= 300) {
        // Last 5 minutes - highlight in warning color
        display.classList.add('text-warning');
        display.classList.add('fw-bold');
        display.style.opacity = '1';
    }
}

/**
 * Convert a time string (HH:MM:SS) to seconds
 * @param {string} timeString - Time in format HH:MM:SS
 * @return {number} Time in seconds
 */
function timeStringToSeconds(timeString) {
    const [hours, minutes, seconds] = timeString.split(':').map(Number);
    return (hours * 3600) + (minutes * 60) + seconds;
}

/**
 * Save the current timer state to localStorage
 * @param {number} testId - Test ID
 * @param {number} timeRemaining - Time remaining in seconds
 */
function saveTimerState(testId, timeRemaining) {
    localStorage.setItem(`test_${testId}_timer`, timeRemaining.toString());
}

/**
 * Load timer state from localStorage
 * @param {number} testId - Test ID
 * @return {number|null} Time remaining in seconds or null if not found
 */
function loadTimerState(testId) {
    const saved = localStorage.getItem(`test_${testId}_timer`);
    return saved ? parseInt(saved, 10) : null;
}

/**
 * Clear saved timer state from localStorage
 * @param {number} testId - Test ID
 */
function clearTimerState(testId) {
    localStorage.removeItem(`test_${testId}_timer`);
}
