document.onload = makeButtonClickable();

function makeButtonClickable() {
    var button = document.getElementById('hello');
    button.onclick = function () {
        new QWebChannel(qt.webChannelTransport, function (channel) {
            channel.objects.bridge.respond('button clicked!!');
        });
    }
}
