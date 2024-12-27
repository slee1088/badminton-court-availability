// Function to fetch and parse the CSV data
async function fetchTimesData() {
  const url = "https://storage.googleapis.com/badminton-bookings/alpha_auburn_bookings.csv";

  try {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const csvText = await response.text();
    const timesData = parseCSV(csvText);

    console.log(timesData); // Output the parsed data to check the result
    return timesData;
  } catch (error) {
    console.error("Error fetching or parsing CSV data:", error);
  }
}

// Function to parse CSV into an array of objects
function parseCSV(csvText) {
  const lines = csvText.split("\n");
  const headers = lines[0].split(",").map(header => header.trim());
  const rows = lines.slice(1);

  const data = rows
    .filter(row => row.trim() !== "") // Skip empty rows
    .map(row => {
      const values = row.split(",").map(value => value.trim());
      const entry = {};
      headers.forEach((header, index) => {
        entry[header] = values[index];
      });
      return entry;
    });

  return data;
}

// Call the function and store the data in a variable
let timesData = null;

fetchTimesData().then(data => {
  timesData = data;
});
