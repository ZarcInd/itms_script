
<?php

use Workerman\Worker;

require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/stats/stats.php';

$address = '0.0.0.0';
$port = 1047;

$pdo = null;

function connectDB()
{
    global $pdo;

    // Database connection
    $dsn = 'mysql:host=127.0.0.1:3306;dbname=itms_primeedg';
    $username = 'itms_primeedg';
    $password = '123';

    try {
        $pdo = new PDO($dsn, $username, $password);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        echo "DB Connection Success\n";
    } catch (PDOException $e) {
        echo 'Connection failed: ' . $e->getMessage() . "\n";
    }
}

connectDB();

// Function to return null if current value is not int
function intOrNull($val)
{
    $res = filter_var($val, FILTER_VALIDATE_INT);
    if ($res === false) return null;
    return $res;
}

// Function to return null if current value is not int
function decimalOrNull($val)
{
    $res = filter_var($val, FILTER_VALIDATE_FLOAT);
    if ($res === false) return null;
    return $res;
}

// Function to get date as integer
function getDateAsInteger($year, $month, $day)
{
    return
        intval($year) * 10000 +
        intval($month) * 100 +
        intval($day);
}

// Function to calculate partition key as integer
function getPartitionKey($utc_date)
{
    if (empty($utc_date)) {
        return null;
    }
    $utc_date = str_replace("/", "-", $utc_date);
    $utc_date = explode("-", $utc_date);
    if (count($utc_date) != 3) {
        return null;
    }
    try {
        // YYYY, MM, DD format
        if (strlen($utc_date[0]) == 4) {
            return getDateAsInteger($utc_date[0], $utc_date[1], $utc_date[2]);
        }
        // DD, MM, YYYY format
        else if (strlen($utc_date[2]) == 4) {
            return getDateAsInteger($utc_date[2], $utc_date[1], $utc_date[0]);
        }
    } catch (\Throwable $e) {
        logError("Error parsing data for partition key");
    }

    return null;
}

// Function to return the json block
function generateJSON($message, $data = null)
{
    return ["message" => $message, "data" => $data];
}

// Function to insert data into db into structured way
function insert_data_db($data_string)
{
    global $pdo;

    if (is_null($pdo)) {
        return generateJSON("Connection to DB failed\n");
    }

    try {
        // Remove trailing `#` since we have int as last column
        $data_string = rtrim($data_string, "#");
        $data_array = explode(',', $data_string);
        $device_type = "UNKNOWN";
        if (count($data_array) >= 3) {
            $device_type = $data_array[2];
        }

        if ($device_type == "VTS") {


            // Prepared statement for inserting data
            $stmt = $pdo->prepare("
                INSERT INTO itms_data (
                    packet_header, mode, device_type, packet_type, firmware_version,
                    device_id, ignition, driver_id, time, date, 
                    gps, lat, lat_dir, lon, lon_dir, 
                    speed_knots, network, route_no, speed_kmh, odo_meter, 
                    Led_health_1, Led_health_2, Led_health_3, Led_health_4, partition_key
                ) VALUES (
                    ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?
                )
            ");

            // Ensure the data array has the correct number of elements
            if (count($data_array) >= 20) {
                // Bind parameters and execute statement
                $stmt->execute([
                    $data_array[0],
                    $data_array[1],
                    $data_array[2],
                    $data_array[3],
                    $data_array[4],
                    $data_array[5],
                    $data_array[6],
                    intOrNull($data_array[7]),
                    $data_array[8],
                    $data_array[9],
                    $data_array[10],
                    decimalOrNull($data_array[11]),
                    $data_array[12],
                    decimalOrNull($data_array[13]),
                    $data_array[14],
                    intOrNull($data_array[15]),
                    intOrNull($data_array[16]),
                    $data_array[17],
                    decimalOrNull($data_array[18]),
                    decimalOrNull($data_array[19]),
                    intOrNull($data_array[20] ?? ""),
                    intOrNull($data_array[21] ?? ""),
                    intOrNull($data_array[22] ?? ""),
                    intOrNull($data_array[23] ?? ""),
                    getPartitionKey($data_array[9]),
                ]);
                 
              if($data_array[3]=='LP')
              {
                 $check = $pdo->prepare("SELECT * FROM itms_data_update WHERE device_id = ?");
$check->execute([$data_array[5]]);
$check_num_row = $check->rowCount(); 



               
                 
                if($check_num_row > 0)
                {
                    //udpate 
						$stmt_update = $pdo->prepare("
    UPDATE itms_data_update SET
        packet_header = ?,
        mode = ?,
        device_type = ?,
        packet_type = ?,
        firmware_version = ?,
        ignition = ?,
        driver_id = ?,
        time = ?,
        date = ?,
        gps = ?,
        lat = ?,
        lat_dir = ?,
        lon = ?,
        lon_dir = ?,
        speed_knots = ?,
        network = ?,
        route_no = ?,
        speed_kmh = ?,
        odo_meter = ?,
        Led_health_1 = ?,
        Led_health_2 = ?,
        Led_health_3 = ?,
        Led_health_4 = ?,
        partition_key = ?
    WHERE device_id = ?
");

$stmt_update->execute([
    $data_array[0],
    $data_array[1],
    $data_array[2],
    $data_array[3],
    $data_array[4],
    $data_array[6],
    intOrNull($data_array[7]),
    $data_array[8],
    $data_array[9],
    $data_array[10],
    decimalOrNull($data_array[11]),
    $data_array[12],
    decimalOrNull($data_array[13]),
    $data_array[14],
    intOrNull($data_array[15]),
    intOrNull($data_array[16]),
    $data_array[17],
    decimalOrNull($data_array[18]),
    decimalOrNull($data_array[19]),
    intOrNull($data_array[20] ?? ""),
    intOrNull($data_array[21] ?? ""),
    intOrNull($data_array[22] ?? ""),
    intOrNull($data_array[23] ?? ""),
    getPartitionKey($data_array[9]),
    $data_array[5], // device_id in WHERE clause
]);

                }
                else{
                    //insert
                     $stmt_update = $pdo->prepare("
                        INSERT INTO itms_data_update (
                            packet_header, mode, device_type, packet_type, firmware_version,
                            device_id, ignition, driver_id, time, date, 
                            gps, lat, lat_dir, lon, lon_dir, 
                            speed_knots, network, route_no, speed_kmh, odo_meter, 
                            Led_health_1, Led_health_2, Led_health_3, Led_health_4, partition_key
                        ) VALUES (
                            ?, ?, ?, ?, ?, 
                            ?, ?, ?, ?, ?, 
                            ?, ?, ?, ?, ?, 
                            ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?
                        )
                    ");

                     $stmt_update->execute([
                    $data_array[0],
                    $data_array[1],
                    $data_array[2],
                    $data_array[3],
                    $data_array[4],
                    $data_array[5],
                    $data_array[6],
                    intOrNull($data_array[7]),
                    $data_array[8],
                    $data_array[9],
                    $data_array[10],
                    decimalOrNull($data_array[11]),
                    $data_array[12],
                    decimalOrNull($data_array[13]),
                    $data_array[14],
                    intOrNull($data_array[15]),
                    intOrNull($data_array[16]),
                    $data_array[17],
                    decimalOrNull($data_array[18]),
                    decimalOrNull($data_array[19]),
                    intOrNull($data_array[20] ?? ""),
                    intOrNull($data_array[21] ?? ""),
                    intOrNull($data_array[22] ?? ""),
                    intOrNull($data_array[23] ?? ""),
                    getPartitionKey($data_array[9]),
                ]);      
                }
              }

                $returnData = [
                    "device_type" => "VTS",
                    "pkt_type" => $data_array[3],
                    "device_id" => $data_array[5],
                    "firmware_version" => $data_array[4], 
                    "time" => $data_array[8],
                    "date" => $data_array[9],
                ];

                return generateJSON("VTS Data inserted successfully.\n", $returnData);
            } else {
                return generateJSON("Invalid data format.\n");
            }
        } else if ($device_type == "CAN") {
            // Prepared statement for inserting data
            $stmt = $pdo->prepare("
                INSERT INTO itms_can_data (
                    packet_header, mode, device_type, packet_type, firmware_version,
                    device_id, time, date, speed_kmh, oil_pressure,
                    partition_key
                ) VALUES (
                    ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?,
                    ?
                )
            ");

            // Ensure the data array has the correct number of elements
            if (count($data_array) >= 10) {
                // Bind parameters and execute statement
                $stmt->execute([
                    $data_array[0],
                    $data_array[1],
                    $data_array[2],
                    $data_array[3],
                    $data_array[4],
                    $data_array[5],
                    $data_array[6],
                    $data_array[7],
                    intOrNull($data_array[8]),
                    intorNull($data_array[9]),
                    getPartitionKey($data_array[7]),
                ]);

                return generateJSON("CAN Data inserted successfully.\n");
            } else {
                return generateJSON("Invalid data format.\n");
            }
        } else {
            return generateJSON("Unknown data packet\n" . $data_string . "\n");
        }
    } catch (PDOException $e) {
        $exception_message = $e->getMessage();
        if (strpos($exception_message, 'server has gone away') !== false) {
            connectDB();
            return insert_data_db($data_string);
        }
        return generateJSON("Connection failed: " . $e->getMessage() . "\n");
    }
}

//Function to insert data into db
function insert_raw_data_db($data_string)
{
    global $pdo;

    if (is_null($pdo)) {
        return "Connection to DB failed\n";
    }

    try {
        // Prepared statement for inserting data
        $stmt = $pdo->prepare("
            INSERT INTO raw_data_logs (
                raw_data
            ) VALUES (
                ?
            )
        ");

        $stmt->execute([$data_string]);

        return "Data inserted successfully.\n";
    } catch (PDOException $e) {
        $exception_message = $e->getMessage();
        if (strpos($exception_message, 'server has gone away') !== false) {
            connectDB();
            return insert_raw_data_db($data_string);
        }
        return "Connection failed: " . $e->getMessage() . "\n";
    }
}

// Create a TCP server listening on port 1047
$tcp_worker = new Worker("tcp://0.0.0.0:1047");

// Set process count to handle multiple connections
$tcp_worker->count = 0; // Adjust based on CPU cores

// Handle incoming messages
$tcp_worker->onMessage = function ($connection, $input) {
    // Clean up the input string
    $input = trim($input);
    // $output = insert_data_db($input);
    // $output = insert_raw_data_db($input);
    // echo $output;
    $output2 = insert_data_db($input);
    echo ($output2["message"] ?? "NA");
// Extract device ID (IMEI) from output
    
    $data_array = explode(',', $input); // Adjust delimiter if necessary
    $firmware_version = $data_array[4] ?? null;
    $device_id = $data_array[5] ?? null;
    
    // Define your current batch of 50 IMEIs
$current_batch_imeis = [ ];
    // If firmware is "new", do NOT send a response
    if ($firmware_version !== "IPC_MTC_v1.23_c") {
    // Only send response if the device is in the current batch
    if (in_array($device_id, $current_batch_imeis)) {
        // Send customized response with device ID
        $response_message ="&PEIS,OTA,{$device_id},DD/MM/YY,HH:MM:SS,13.127.240.235-21-itms_ftp-Prime_123-/MTC_IPC_V1.23_c/#\r\n";
        $connection->send($response_message);
    }
}

    // Construct response message
   // $response_message =  "no string";
   // $connection->send($response_message);
    
    $connection->close(); # Check if we can improve performance by keeping connection alive to save (SYN, SYN-ACK, ACK) part

    echo json_encode($output2);

    updateVTSData($output2["data"] ?? null);
};

// Run the worker
Worker::runAll();
