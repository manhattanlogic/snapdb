<?php
$data = file_get_contents("http://localhost:8090/get_vid?x=" . $_REQUEST["x"] . "&y=" . $_REQUEST["y"]);
echo($data);
?>
