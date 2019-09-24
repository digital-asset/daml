### Dev Setup
The dev setup is not automated, to get a local dev env follow steps.

    cd compiler/damlc/daml-visual/src/DA/d3-dev
    wget https://github.com/alexandersimoes/d3plus/releases/download/v1.9.8/d3plus.zip
    unzip d3plus.zip
    python -m http.server --directory .
    open http://0.0.0.0:8000/d3-dev.html

If there are any changes to be made to the JS, they will have to be made to the webpage template within `Visual.hs` module.

Note: We can automate this with using npm/yarn but is not done as of yet as we are not dealing with a lot of JS/HTML/CSS code. If we add more customization it might be worth the effort to setup a automated dev setup.

### About D3 and D3Plus and versions used in this project
[D3JS](https://d3js.org/) is a popular Data visualization library and [D3Plus](https://github.com/alexandersimoes/d3plus) gives simple api with reasonable defaults to draw SVG.

The version currently we are using is `1.9.8` and the next version of `D3Plus` is `2.0.0` which takes compositional style i.e. lets us pick the libs (tooltips, legends etc.) that we need. The reason we are not using latest(`2.0.0`) is the composition of libs is not very well documented and lacks advanced examples and also it is currently labeled as beta.

### Why are JS files packed into one HTML and inlined?
- This makes sharing the file a lot easy
- We don't have to worry about webserver on client machine as we can get away with just opening a file in browser vs referencing local files on disk which is restricted by CQRS policy.
- There might be performance issues with larger projects.


### Helpful Links
* [Getting Started](https://d3plus.org/blog/getting-started/2014/06/12/getting-started-1/)
* [Live Examples](https://d3plus.org/examples/)
* [Documentation](https://github.com/alexandersimoes/d3plus/wiki)
* [Google Group Discussions](https://groups.google.com/forum/#!forum/d3plus)