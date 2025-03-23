const fs = require('fs');
const path = require('path');

function fixPaths(dir) {
  const files = fs.readdirSync(dir);
  
  files.forEach(file => {
    const filePath = path.join(dir, file);
    const stat = fs.statSync(filePath);
    
    if (stat.isDirectory()) {
      fixPaths(filePath);
    } else if (file.endsWith('.html') || file.endsWith('.js')) {
      let content = fs.readFileSync(filePath, 'utf8');
      
      // Fix paths in HTML files
      if (file.endsWith('.html')) {
        content = content.replace(/src="\//g, 'src="./');
        content = content.replace(/href="\//g, 'href="./');
      }
      
      // Fix paths in JS files
      if (file.endsWith('.js')) {
        content = content.replace(/path:\s*"\//g, 'path: "./');
      }
      
      fs.writeFileSync(filePath, content);
    }
  });
}

const outDir = path.join(__dirname, '..', 'out');
fixPaths(outDir);
console.log('Paths fixed successfully.');

