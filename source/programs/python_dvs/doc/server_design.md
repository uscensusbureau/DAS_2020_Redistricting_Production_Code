DVS Web Interface
=================

The DVS web interface uses the following building blocks:



Web Components for DVS Search
-----------------------------

We use the following Custom elements, as explained here:
* https://developers.google.com/web/fundamentals/web-components/customelements
* https://developer.mozilla.org/en-US/docs/Web/API/Window/customElements

We may also use Web Components with Angular:
* https://indepth.dev/angular-web-components-a-complete-guide/


### `x-dvs-hash`
This is the basic custom element for displaying hashes in the DVS user interface. It is called like this:

```
<x-dvs-hash alg='md5' value='f1e0961a881c8e15e4e77587255caf5b' more='base64encoded-URL'><x-dvs-hash/>
```
Where:

* `x-dvs-hash` the custom element tag.
* `alg='md5'` the algorithm of the hash
* `value='...'` the hexademical encoding of the hash
* `more='base64encoded-URL'` a Base64-encoded URL that, when decoded, is a link that can be followed for more information about this hash.---(To Be Implemented Later, important for the links that go to github locations)

A Javascript class with various functions in the `dvs_search.js` file makes this custom element work:

`class DVSHash extends HTMLElement` --- adds the `x-dvs-hash` web component
`connectedCallback()` --- Called when a new x-dvs-hash element is created (for example, when the document is first loaded). Does the initial setup of the elements that are used to render hashes (e.g. Event listeners for the dots after the hash, the tooltip, etc)

`get observedAttributes()` --- Tells the component what extra properties to observe in the tag itself (e.g. value and alg)

`showTooltip() and showHash()` ---Functions defining the events for the tooltip and expanding/copying the hash

`get/set [alg or value]()`--- Getters and Setters for the new alg and value properties

`var template`--- Sets up the template HTML structure and shadow css styling of how the x-dvs-hash component is displayed


