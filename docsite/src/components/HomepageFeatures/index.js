import React from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';

const FeatureList = [
  {
    title: 'Delta or Iceberg? You choose!',
    Img: require('@site/static/img/fox1.png').default,
    description: (
      <>
        Whitefox is a protocol and a server that enables easy data sharing
        on top of table formats.
      </>
    ),
  },
  {
    title: 'Completely compatible with Delta Sharing',
    Img: require('@site/static/img/fox2.png').default,
    description: (
      <>
        Whitefox was build from the ground up to be compatible with Delta Sharing protocol.
        New features are added on top of Delta Sharing protocol, so you can use
        Whitefox as a Delta Sharing server and still be compatible with Delta Sharing clients.
      </>
    ),
  },
  {
    title: 'Cloud native',
    Img: require('@site/static/img/fox3.png').default,
    description: (
      <>
        Whitefox is a cloud native application, it can be deployed on Kubernetes
        or any other container orchestration system. It's a production ready replacement
        of Delta Sharing server.
      </>
    ),
  },
];

function Feature({Img, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <img src={Img} alt="Logo" className={styles.featureImg}/>
      </div>
      <div className="text--center padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
