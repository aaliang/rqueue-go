client bindings for [rqueue](https://github.com/aaliang/rqueue) in golang

####Example usage
```.go

import (
  "fmt"
	"github.com/aaliang/rqueue-go/rqueue"
	"time"
)

func main() {
  config := Config{host: "0.0.0.0", port: 6567}
  rqueue, err := NewClient(config)
  if err != nil {
    log.Fatal(err)
  } else {
    rqueue.Subscribe("hello")
    // for now there is no ACK to Subscribes
    time.Sleep(100 *time.Millisecond)
    rqueue.Notify("hello", "world")
    
    go func() {
      for {
        mess, err := rqueue.GetMessage()
        if err == nil {
          fmt.Println(mess.topic)
          fmt.Println(mess.message)
        }
      }
    }
}
```
